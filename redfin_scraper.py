from __future__ import division
import csv
from selenium import webdriver
# from pyvirtualdisplay import Display
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from pyzipcode import ZipCodeDatabase
from itertools import izip_longest
from datetime import datetime as dt
import pandas as pd
import time
import pickle

# display = Display(visible=0, size=(1024, 768))
# display.start()

chrome_options = Options()
chrome_options.add_extension("./proxy.zip")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--start-maximized")

OUT_DIR = './data/'
zcdb = ZipCodeDatabase()
# zips = [zc.zip for zc in zcdb.find_zip()]
zips = ['94609']
startDate = dt.now().strftime('%Y-%m-%d')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]




def getChromeDriver(chromeOptions=None):
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.implicitly_wait(10)
    return driver


def switchToTableView(driver):
    try:
        button = driver.find_element(
            By.XPATH, '''//span[@data-rf-test-name="tableOption"]''')
        button.click()
        return
    except:
        return


def checkForLoginPrompt(driver):
    try:
        loginPrompt = driver.find_element(
            By.XPATH, '//div[@data-rf-test-name="dialog-close-button"]')
        print('Detected login prompt. Will try to close.')
    except:
        return False
    loginPrompt.click()
    return True


def checkForPopUp(driver):
    try:
        popup = driver.find_element(
            By.XPATH,
            '//a[@href="https://www.redfin.com' +
            '/buy-a-home/classes-and-events"]')
        popup.find_element(By.XPATH, '../../../img').click()
        return
    except NoSuchElementException:
        return


def checkForMap(driver):
    noMap = True
    while noMap:
        try:
            driver.find_element(
                By.XPATH, '//div[@class="GoogleMapView"]')
            noMap = False
        except:
            print('No map detected. Refreshing browser.')
            driver.refresh()
            time.sleep(5)
    return


def waitForProgressBar(driver):
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located(
                (By.XPATH,
                    '//div[@data-rf-test-name="progress-bar-text"]')))
    except:
        print('Never any progress bar')
        checkForMap(driver)
        return
    try:
        WebDriverWait(driver, 30).until(
            EC.invisibility_of_element_located(
                (By.XPATH,
                    '//div[@data-rf-test-name="progress-bar-text"]')))
    except:
        print('Timed out waiting for progress bar to finish.')
        print('Refreshing browser.')
        driver.refresh()
    return


def checkForFlyout(driver):
    flyOut = True
    try:
        selectedRow = driver.find_element(
            By.XPATH, '//tr[@class="selected tableRow"]')
    except:
        print('No rows detected. Refreshing browser.')
        driver.refresh()
        return
    selectedRowNum = int(selectedRow.get_attribute('id').split('_')[1])
    while flyOut:
        try:
            driver.find_element(
                By.XPATH, '//div[@class="clickableHome MultiUnitFlyout"]')
            print('Flyout menu detected')
            nextRow = driver.find_element(
                By.XPATH,
                '//tr[@id="ReactDataTableRow_{0}"]'.format(selectedRowNum + 1))
            print('Clicking next row to close flyout')
            actions = ActionChains(driver)
            actions.move_to_element(nextRow).perform()
            nextRow.click()
            print('Flyout should be gone.')
            checkForLoginPrompt(driver)
            selectedRowNum += 1
        except NoSuchElementException:
            flyOut = False
    return


def ensurePageReady(driver):
    checkForLoginPrompt(driver)
    waitForProgressBar(driver)
    switchToTableView(driver)
    checkForPopUp(driver)
    checkForFlyout(driver)
    return


def goToRedfin(zipcode, chromeOptions=None):
    driver = getChromeDriver(chromeOptions)
    zc = zipcode
    url = "http://www.redfin.com/zipcode/" + zc + \
        "/filter/include=sold-all"
    driver.get(url)
    switchToTableView(driver)
    ensurePageReady(driver)
    return driver


def getClusters(driver):
    clusters = driver.find_elements(
        By.XPATH, '//div[@class="numHomes"]')
    return clusters


def getListingCount(driver):
    elemText = driver.find_elements(
        By.XPATH, '//div[@class="homes summary"]')[0].text
    if 'of' in elemText:
        countStr = elemText.split('of')[1].split('Homes')[0].strip()
    else:
        countStr = elemText.split()[1]
    assert countStr.isdigit()
    return int(countStr)


def getSubClusters(driver, mainClusterDict, mainClusterNo):
    i = mainClusterNo
    mainClusterUrl = driver.current_url
    totalCountFromSubClusters = 0
    subClusters = getClusters(driver)
    numSubClusters = len(subClusters)
    mainClusterDict[i].update({'numSubClusters': numSubClusters})
    subClusterDict = mainClusterDict[i]['subClusters']
    count = mainClusterDict[i]['count']
    origCount = count
    subClustersUnder350 = []
    subClustersOver350 = []
    allUrls = []
    for j in range(len(subClusters)):
        if (j in subClusterDict.keys()) and (subClusterDict[j]['complete']):
            continue
        else:
            if j not in subClusterDict.keys():
                subClusterDict[j] = {'complete': False}
        assert len(subClusters) == numSubClusters
        assert driver.current_url == mainClusterUrl
        assert origCount - 2 <= count <= origCount + 2
        print('Clicking {0} of {1} subClusters in main cluster {2}'.format(
            j + 1, len(subClusters), i + 1))
        try:
            subClusters[j].click()
            subClusterDict[j].update({'clickable': True})
            print('Subcluster {0}.{1} clicked!'.format(i + 1, j + 1))
            ensurePageReady(driver)
        except:
            print('Could not click subcluster {0}.{1}.'.format(
                i + 1, j + 1))
            subClusterDict[j].update({'clickable': False})
            continue
        newCount = getListingCount(driver)
        sttm = time.time()
        while (newCount == count) and (time.time() - sttm < 30):
            newCount = getListingCount(driver)
        count = getListingCount(driver)
        subClusterDict[j].update({'count': count, 'url': driver.current_url})
        totalCountFromSubClusters += count
        if count > 345:
            print('Subcluster {0}.{1} has more than 350 listings'.format(
                i + 1, j + 1))
            subClustersOver350.append(j)
            driver.get(mainClusterUrl)
            ensurePageReady(driver)
        else:
            print('Subcluster {0}.{1} has less than 350 listings'.format(
                i + 1, j + 1))
            print('Getting listing urls for subcluster {0}.{1}'.format(
                i + 1, j + 1))
            listingUrls = getAllUrls(driver)
            allUrls += listingUrls
            assert count - 2 <= len(listingUrls) <= count + 2
            subClusterDict[j].update({'listingUrls': listingUrls})
            subClustersUnder350.append(j)
            driver.get(mainClusterUrl)
            ensurePageReady(driver)
        subClusterDict[j]['complete'] = True
        newCount = getListingCount(driver)
        sttm = time.time()
        while (newCount == count) and (time.time() - sttm < 30):
            newCount = getListingCount(driver)
        count = getListingCount(driver)
        subClusters = getClusters(driver)

    mainClusterDict[i].update(
        {'subClustersOver350': subClustersOver350,
            'numSubClustersOver350': len(subClustersOver350)})
    uniqueUrls = list(set(allUrls))
    return subClustersUnder350, \
        subClustersOver350, \
        totalCountFromSubClusters, \
        uniqueUrls


def getMainClusters(driver, mainClusterDict):
    uniqueUrls = []
    origUrl = driver.current_url
    totalCountFromClusters = 0
    clusters = getClusters(driver)
    numClusters = len(clusters)
    count = getListingCount(driver)
    origCount = count
    mainClustersUnder350 = []
    mainClustersOver350 = []

    for i in range(numClusters):
        if (i in mainClusterDict.keys()) and (mainClusterDict[i]['complete']):
            continue
        else:
            if i not in mainClusterDict.keys():
                mainClusterDict[i] = {'complete': False, 'subClusters': {}}
        assert len(clusters) == numClusters
        assert origCount - 3 <= count <= origCount + 3
        print('Clicking {0} of {1} main clusters'.format(
            i + 1, len(clusters)))
        try:
            clusters[i].click()
            mainClusterDict[i].update({'clickable': 'yes'})
            print('We clicked it!')
            ensurePageReady(driver)
        except:
            print('Could not click it.')
            mainClusterDict[i].update({'clickable': 'no'})
            continue
        newCount = getListingCount(driver)
        sttm = time.time()
        while (newCount == count) and (time.time() - sttm < 30):
            newCount = getListingCount(driver)
        count = getListingCount(driver)
        mainClusterDict[i].update({'count': count, 'url': driver.current_url})
        totalCountFromClusters += count
        if count > 345:
            print('Main cluster {0} has more than 350 listings'.format(i + 1))
            print('Traversing subclusters')
            subClustersUnder350, subClustersOver350, \
                totalCountFromSubClusters, \
                uniqueSubClusterUrls = getSubClusters(
                    driver, mainClusterDict, i)
            uniqueUrls += uniqueSubClusterUrls
            pctObtained = len(uniqueSubClusterUrls) / count
            mainClusterDict[i].update(
                {'pctObtained': pctObtained,
                    'listingUrls': uniqueSubClusterUrls})
            mainClustersOver350.append(i)
            driver.get(origUrl)
            ensurePageReady(driver)
        else:
            print('Main cluster {0} has less than 350 listings'.format(i + 1))
            print('Getting the individual listing urls.')
            listingUrls = getAllUrls(driver)
            assert count - 2 <= len(listingUrls) <= count + 2
            pctObtained = len(listingUrls) / count
            mainClusterDict[i].update(
                {'subClusterDict': None,
                    'pctObtained': pctObtained,
                    'listingUrls': listingUrls})
            mainClustersUnder350.append(i)
            driver.get(origUrl)
            ensurePageReady(driver)
        mainClusterDict[i]['complete'] = True
        newCount = getListingCount(driver)
        sttm = time.time()
        while (newCount == count) and (time.time() - sttm < 30):
            newCount = getListingCount(driver)
        count = getListingCount(driver)
        clusters = getClusters(driver)

    return mainClustersUnder350, \
        mainClustersOver350, \
        totalCountFromClusters, \
        uniqueUrls


def getPageUrls(driver):
    zcUrls = []
    try:
        pageLinks = driver.find_elements(By.XPATH, '''//tbody[@class="tableList"]/tr/
            td[@class="column column_1 col_address"]/div/a''')
        for pageLink in pageLinks:
            url = pageLink.get_attribute('href')
            zcUrls.append(url)
    except:
        print("No page links for")

    return zcUrls


def getAllUrls(driver):

    allUrls = []

    firstUrls = getPageUrls(driver)
    if len(firstUrls) == 0:
        print("No listings for " + zc)
        return allUrls
        # noListings.append(zc)

    allUrls += firstUrls
    print('First page got {0} urls'.format(len(firstUrls)))
    nextPages = driver.find_elements(
        By.XPATH, '''//*[@id="sidepane-root"]/div[2]/''' +
        '''div/div[4]/div[2]/a[contains(@class,"goToPage")]''')
    if len(nextPages) == 0:
        return allUrls

    actions = ActionChains(driver)
    print('Scraping {0} more pages'.format(len(nextPages) - 1))

    for k in range(1, len(nextPages)):
        thisPage = driver.find_element(
            By.XPATH,
            '//a[@data-rf-test-id="react-data-paginate-page-{0}"]'.format(k))
        actions.move_to_element(thisPage).perform()
        thisPage.click()
        checkForLoginPrompt(driver)
        time.sleep(2)
        nextUrls = getPageUrls(driver)
        print('Page {0} got {1} urls'.format(k + 1, len(nextUrls)))
        allUrls += nextUrls

    return allUrls


def traverse(driver):
    urls = []
    if getListingCount(driver) > 350:
        clusters = getClusters(driver)
        for i in range(len(clusters)):
            clusters[i].click()
            urls += traverse(driver)
            driver.back()
            clusters = getClusters(driver)
    else:
        urls += getAllUrls(driver)
    return urls


def getEventDate(htmlEventElement):
    rawDateStr = htmlEventElement.find_element(
        By.XPATH, './/td[contains(@class,"date-col")]').text
    dateStr = dt.strptime(rawDateStr, '%b %d, %Y').strftime('%Y-%m-%d')
    return dateStr


def getEventPrice(htmlEventElement):
    price = htmlEventElement.find_element(
        By.XPATH, './/td[contains(@class,"price-col")]') \
        .text.strip('$').replace(',', '')
    return price.encode('utf-8')


def getEventsFromListing(driver):
    events = []
    info = driver.find_element(
        By.XPATH, '//div[contains(@class, "main-stats inline-block")]')
    streetAddr = info.find_element(
        By.XPATH, './/span[@itemprop="streetAddress"]').text
    cityStateZip = info.find_element(
        By.XPATH, './/span[@class="citystatezip"]')
    city = cityStateZip.find_element(
        By.XPATH, './/span[@class="locality"]').text.strip(',')
    state = cityStateZip.find_element(
        By.XPATH, './/span[@class="region"]').text
    zipcode = cityStateZip.find_element(
        By.XPATH, './/span[@class="postal-code"]').text
    lat = info.find_element(
        By.XPATH, './/span[@itemprop="geo"]/meta[@itemprop="latitude"]') \
        .get_attribute('content')
    lon = info.find_element(
        By.XPATH, './/span[@itemprop="geo"]/meta[@itemprop="longitude"]') \
        .get_attribute('content')
    try:
        bedsInfo = info.find_element(
            By.XPATH, '''.//span[contains(text(),"Bed")]/..''') \
            .text.split('\n')
        beds = bedsInfo[0].encode('utf-8')
    except:
        beds = None

    try:
        bathsInfo = info.find_element(
            By.XPATH, '''.//span[contains(text(),"Bath")]/..''') \
            .text.split('\n')
        baths = bathsInfo[0].encode('utf-8')
    except:
        baths = None

    try:
        sqftInfo = info.find_element(
            By.XPATH, '''.//span[@class="sqft-label"]/..
            /span[@class="main-font statsValue"]''').text
        sqft = sqftInfo.replace(',', '').encode('utf-8')
    except:
        sqft = None

    try:
        yearBuiltInfo = info.find_element(
            By.XPATH, '''.//span[text()="Built: "]/..''').text.split(': ')
        yearBuilt = yearBuiltInfo[1].encode('utf-8')
    except:
        yearBuilt = None

    try:
        keyDetails = driver.find_element(
            By.XPATH, '''//div[@class="keyDetailsList"]
            /div/span[text()="MLS#"]/..''').text.split('\n')
        if keyDetails[0] == "MLS#":
            mls = keyDetails[1].encode('utf-8')
        else:
            mls = None
    except:
        mls = None

    factsTable = driver.find_element(
        By.XPATH, '//div[@class="facts-table"]')
    factList = factsTable.text.split('\n')
    factDict = dict(izip_longest(*[iter(factList)] * 2, fillvalue=''))
    for k, v in factDict.items():
        factDict[k] = v.encode('utf-8')

    if beds is None:
        beds = factDict['Beds']

    if baths is None:
        baths = factDict['Baths']

    if sqft is None:
        sqft = factDict['Total Sq. Ft.'].replace(',', '')

    lotSize = factDict['Lot Size']
    if yearBuilt is None:
        yearBuilt = factDict['Year Built']

    propType = factDict['Style']
    apn = factDict['APN']
    staticAttrs = [mls, apn, streetAddr, city, state, zipcode,
                   lat, lon, beds, baths, sqft, lotSize, propType,
                   yearBuilt, driver.current_url]
    historyRows = driver.find_elements(
        By.XPATH, '//*[contains(@id,"propertyHistory")]')
    lastEvent = None
    saleDt = None
    salePrice = None
    for i, historyRow in enumerate(historyRows):
        print i
        if 'Sold' in historyRow.text:
            curSaleDt = getEventDate(historyRow)
            curSalePrice = getEventPrice(historyRow)
            if lastEvent == 'sale' and curSaleDt != saleDt:
                events.append(
                    staticAttrs + [saleDt, salePrice, None, None])
            if i + 1 == len(historyRows):
                events.append(
                    staticAttrs + [curSaleDt, curSalePrice, None, None])
            lastEvent = 'sale'
            saleDt, salePrice = curSaleDt, curSalePrice
        elif 'Listed' in historyRow.text and lastEvent == 'sale':
            listDt = getEventDate(historyRow)
            listPrice = getEventPrice(historyRow)
            events.append(
                staticAttrs + [saleDt, salePrice, listDt, listPrice])
            lastEvent = 'listing'
        else:
            continue

    return events


def pickleClusterDict(clusterDict):
    pickle.dump(clusterDict, open('main_cluster_dict.pkl','wb'))


def writeCsv(outfile, row):

    with open(outfile, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(row)


# def writeDb(eventList):

#     dbname = 'redfin'
#     host = 'localhost'
#     port = 5432
#     conn_str = "dbname={0} host={1} port={2}".format(dbname, host, port)
#     conn = psycopg2.connect(conn_str)
#     cur = conn.cursor()
#     num_listings = len(eventList)
#     prob_PIDs = []
#     dupes = []
#     writes = []
#     for i, row in eventList:
#         try:
#             cur.execute('''INSERT INTO sales_listings
#                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
#                             (row['pid'],row['date'].to_datetime(),row['region'],row['neighborhood'],
#                             row['rent'],row['bedrooms'],row['sqft'],row['rent_sqft'],
#                             row['longitude'],row['latitude'],row['county'],
#                             row['fips_block'],row['state'],row['bathrooms']))
#             conn.commit()
#             writes.append(row['pid'])
#         except Exception, e:
#             if 'duplicate key value violates unique' in str(e):
#                 dupes.append(row['pid'])
#             else:
#                 prob_PIDs.append(row['pid'])
#             conn.rollback()
#     cur.close()
#     conn.close()
#     return prob_PIDs, dupes, writes


newNotListed = []
noListings = []

for zc in zips:
    fname = OUT_DIR + 'sales_' + zc + '_' + startDate + '.csv'
    print("getting driver for " + zc)
    if zc in not_listed:
        continue

    allZipCodeUrls = []
    driver = goToRedfin(zc, chrome_options)

    print("got driver for " + zc)

    if driver.current_url == 'https://www.redfin.com/out-of-area-signup':
        print('no landing page for ' + zc)
        newNotListed.append(zc)
        continue

    totalListings = getListingCount(driver)
    # mainClusterDict = {}
    mainClusterDict = pickle.load(open('main_cluster_dict.pkl', 'rb'))
    if totalListings < 345:
        allZipCodeUrls = getAllUrls(driver)
    else:
        mainClustersUnder350, mainClustersOver350, \
            totalCountFromClusters, allZipCodeUrls = getMainClusters(
                driver, mainClusterDict)
        numMainClusters = len(mainClusterDict.items())
        numClickableClusters = sum(
            1 for cluster in mainClusterDict.values()
            if cluster['clickable'] == 'yes')
        pctScrapable = round(totalCountFromClusters / totalListings, 3) * 100
        print('{0} of {1} main clusters are clickable'.format(
            numClickableClusters, numMainClusters))
        print('{0} of {1} total listings ({2}%) '.format(
            totalCountFromClusters, totalListings, pctScrapable) +
            'are scrapable from main clusters')
    totalUrlsObtained = len(allZipCodeUrls)
    pctUrlsObtained = totalUrlsObtained / totalListings
    print('{0} of {1} total listings urls ({2}%) were obtained'.format(
        len(allZipCodeUrls), totalListings, pctUrlsObtained))

    break
















    # if totalListings <= 350:
    #     allZipCodeUrls = getAllUrls(driver)

        # firstUrls = getPageUrls(driver)
        # if len(firstUrls) == 0:
        #     print("no listings for " + zc)
        #     noListings.append(zc)

        # allZipCodeUrls += firstUrls
        # nextPages = driver.find_elements(
        #     By.XPATH, '//a[@class="clickable goToPage"]')

        # for page in nextPages:
        #     page.click()
        #     nextUrls = getPageUrls(driver)
        #     allZipCodeUrls += nextUrls

    # else:
    #     clusters = getClusters(driver)
    #     for cluster in clusters:
    #         cluster.click()

    # with open("processed_urls.csv", 'r') as pus:
    #     urls = pus.read().split('\r\n')

    # with open('processed_urls.csv', 'a') as pus:
    #     pus_writer = csv.writer(pus)
    #     with open(fname, 'wb') as f:
    #         writer = csv.writer(f)
    #         for i, url in enumerate(allZipCodeUrls):
    #             if url in urls:
    #                 print('been there done that.')
    #                 continue
    #             print('Scraping events for listing {0} of {1}'.format(
    #                 i + 1, len(allZipCodeUrls)))
    #             try:
    #                 driver.get(url)
    #                 events = getEventsFromListing(driver)
    #                 for j, event in enumerate(events):
    #                     print("writing event {0} of {1}".format(
    #                         j + 1, len(events)))
    #                     writer.writerow(event)
    #             except Exception, e:
    #                 print(Exception, e, url)
    #                 break
    #                 continue
    #             pus_writer.writerow([url])
    #         break




# with open('not_listed.csv', 'wb') as f:
#     wr = csv.writer(f)
#     for zipcode in newNotListed:
#         wr.writerow([zipcode])
