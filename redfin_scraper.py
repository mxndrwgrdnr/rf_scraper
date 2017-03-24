from __future__ import division
import csv
from selenium import webdriver
from pyvirtualdisplay import Display
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
from multiprocessing import Process, Manager

display = Display(visible=0, size=(1024, 768))
display.start()

chrome_options = Options()
chrome_options.add_extension("./proxy.zip")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument("window-size=1024,768")
chrome_options.add_argument("--start-maximized")

dataDir = './data/'
zcdb = ZipCodeDatabase()
# zips = [zc.zip for zc in zcdb.find_zip()]
zips = ['44122']
startDate = dt.now().strftime('%Y-%m-%d')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]




def getChromeDriver(chromeOptions=None):
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    return driver


def switchToTableView(driver):
    try:
        button = driver.find_element(
            By.XPATH, '''//span[@data-rf-test-name="tableOption"]''')
        button.click()
        return
    except:
        return


def checkForLoginPrompt(driver, zipOrClusterId=False):
    try:
        loginPrompt = driver.find_element(
            By.XPATH, '//div[@data-rf-test-name="dialog-close-button"]')
        print('Detected login prompt at {0}. Will try to close.'.format(zipOrClusterId))
    except NoSuchElementException:
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


def checkForMap(driver, zipOrClusterId=False):
    if not zipOrClusterId:
        zipOrClusterId = driver.current_url
    noMap = True
    sttm = time.time()
    tries = 1
    while noMap and time.time() -  sttm < 60:
        try:
            driver.find_element(
                By.XPATH, '//div[@class="GoogleMapView"]')
            noMap = False
        except NoSuchElementException:
            print('No map detected at {0}. Refreshing browser. Try #{1}'.format(
                zipOrClusterId, tries))
            driver.refresh()
            tries += 1
            time.sleep(5)
    if noMap:
        print('Could not load map at {0}'.format(zipOrClusterId))
        return False
    else:
        if tries > 1:
            print('Found map after all at {0}.'.format(zipOrClusterId))
        return True


def waitForProgressBar(driver, zipOrClusterId=False):
    if not zipOrClusterId:
        zipOrClusterId = driver.current_url
    try:
        WebDriverWait(driver, 3).until(
            EC.presence_of_element_located(
                (By.XPATH,
                    '//div[@data-rf-test-name="progress-bar-text"]')))
    except:
        mapFound = checkForMap(driver, zipOrClusterId)
        return mapFound
    try:
        WebDriverWait(driver, 30).until(
            EC.invisibility_of_element_located(
                (By.XPATH,
                    '//div[@data-rf-test-name="progress-bar-text"]')))
    except:
        print('Timed out waiting for progress bar to finish at {0}. Refreshing browser.'.format(zipOrClusterId))
        driver.refresh()
        return False
    return True


def checkForFlyout(driver, zipOrClusterId=False):
    if not zipOrClusterId:
        zipOrClusterId = driver.current_url
    flyOut = True
    try:
        selectedRow = driver.find_element(
            By.XPATH, '//tr[@class="selected tableRow"]')
    except NoSuchElementException:
        print('No rows detected at {0}. Refreshing browser.'.format(zipOrClusterId))
        driver.refresh()
        return
    selectedRowNum = int(selectedRow.get_attribute('id').split('_')[1])
    while flyOut:
        try:
            driver.find_element(
                By.XPATH, '//div[@class="clickableHome MultiUnitFlyout"]')
            print('Flyout menu detected at {0}. Clicking next row to close flyout.'.format(zipOrClusterId))
            nextRow = driver.find_element(
                By.XPATH,
                '//tr[@id="ReactDataTableRow_{0}"]'.format(selectedRowNum + 1))
            actions = ActionChains(driver)
            actions.move_to_element(nextRow).perform()
            nextRow.click()
            print('Flyout should be gone at {0}.'.format(zipOrClusterId))
            checkForLoginPrompt(driver)
            selectedRowNum += 1
        except NoSuchElementException:
            flyOut = False
    return


def ensureMapClickable(driver, zipOrClusterId=False):
    ok = ensurePageScrapable(driver, zipOrClusterId)
    checkForPopUp(driver)
    checkForFlyout(driver, zipOrClusterId)
    return ok


def ensurePageScrapable(driver, zipOrClusterId=False):
    checkForLoginPrompt(driver, zipOrClusterId)
    ok = waitForProgressBar(driver, zipOrClusterId)
    switchToTableView(driver)
    return ok


def acMoveAndClick(driver, element):
    actions = ActionChains(driver)
    actions.move_to_element(element)
    actions.click(element)
    actions.perform()


def goToRedfin(zipcode, chromeOptions=None):
    driver = getChromeDriver(chromeOptions)
    zc = zipcode
    url = "http://www.redfin.com/zipcode/" + zc + \
        "/filter/include=sold-all"
    driver.get(url)
    switchToTableView(driver)
    ensureMapClickable(driver)
    return driver


def goToRedfinViewport(url, chromeOptions=None):
    if not zipOrClusterId:
        zipOrClusterId = url
    driver = getChromeDriver(chromeOptions)
    try:
        driver.get(url)
    except:
        driver.quit()
        return False
    switchToTableView(driver)
    if ensureMapClickable(driver):
        return driver
    else:
        driver.quit()
        return False


def waitForListingsToLoad(driver, count):
    newCount = getListingCount(driver)
    sttm = time.time()
    while (newCount == count) and (time.time() - sttm < 30):
        newCount = getListingCount(driver)


def getClusters(driver):
    clusters = driver.find_elements(
        By.XPATH, '//div[@class="numHomes"]')
    return clusters


def checkForClusters(driver):
    if not len(getClusters(driver)):
        driver.refresh()


def getListingCount(driver):
    elemText = driver.find_elements(
        By.XPATH, '//div[@class="homes summary"]')[0].text
    if 'of' in elemText:
        countStr = elemText.split('of')[1].split('Homes')[0].strip()
    else:
        countStr = elemText.split()[1]
    assert countStr.isdigit()
    return int(countStr)


def instantiateMainClusterDict(zipcode):
    try:
        mcd = pickle.load(
            open(dataDir + 'pickles/' +
                'main_cluster_dict_{0}.pkl'.format(zipcode), 'rb'))
        print('Loaded existing zipcode cluster data.')
    except:
        mcd = {'clusters': {}, 'numClusters': 0, 
                       'numClustersNotClicked': 0, 'clustersNotClicked': [],
                       'listingUrls': []}
    return mcd


def instantiateClusterDict():
    clusterDict = {'complete': False,
        'count': 0,
        'numSubClusters': 0, 
        'numSubClustersNotClicked': 0,
        'subClustersOver350': [],
        'numSubClustersOver350': 0,
        'subClustersNotClicked': [],
        'listingUrls': []}
    return clusterDict


def formatSubClusterDict(complete, url, clickable, count, listingUrls):
    clusterDict = {'complete': complete,
        'url': url,
        'clickable': clickable,
        'count': count,
        'listingUrls': listingUrls}
    return clusterDict



def scrapeSubClusterUrls(parallelDict, mainClusterUrl, mainClusterNo,
                         numSubClusters, subClusterNo, mainClusterCount,
                         chromeOptions):
    i = mainClusterNo
    j = subClusterNo
    subClusterID = '{0}.{1}'.format(i + 1, j + 1)
    complete = False
    url = None
    clickable = None
    count = 0
    listingUrls = []
    try:
        scDriver = goToRedfinViewport(mainClusterUrl, chromeOptions)
        if not scDriver:
            raise Exception
    except:
        print('Subcluster {0} Chrome instance failed to load.'.format(subClusterID))
        parallelDict[j] = formatSubClusterDict(complete, url, clickable, count, listingUrls)
        return
    subClusters = getClusters(scDriver)
    assert len(subClusters) == numSubClusters
    assert scDriver.current_url == mainClusterUrl
    try:
        acMoveAndClick(scDriver, subClusters[j])
        ensurePageScrapable(scDriver, subClusterID)
        if scDriver.current_url == mainClusterUrl:
            acMoveAndClick(scDriver, subClusters[j])
            ensurePageScrapable(scDriver, subClusterID)
            assert scDriver.current_url != mainClusterUrl
        clickable = True
    except:
        clickable = False
        print('Subcluster {0} could not be clicked.'.format(subClusterID))
        complete = True
        scDriver.quit()
        parallelDict[j] = formatSubClusterDict(complete, url, clickable, count, listingUrls)
        return

    waitForListingsToLoad(scDriver, mainClusterCount)
    count = getListingCount(scDriver)
    url = scDriver.current_url
    if count > 345:
        print('Subcluster {0} had more than 350 listings.'.format(subClusterID))
    else:
        listingUrls = getAllUrls(scDriver)
        pctObtained = round(len(listingUrls) / count, 2) * 100
        print(
            'Subcluster {3} returned {0} of {1} ({2}%) listing urls.'.format(
                len(listingUrls), count, pctObtained, subClusterID))

    complete = True
    parallelDict[j] = formatSubClusterDict(complete, url, clickable, count, listingUrls)
    scDriver.quit()
    return


def getSubClustersInParallel(driver, mainClusterDict, mainClusterNo,
                             chromeOptions, zipcode, timeout=30):
    i = mainClusterNo
    mainClusterUrl = driver.current_url
    subClusters = getClusters(driver)
    numSubClusters = len(subClusters)
    print('Found {0} subclusters in cluster {1} in zipcode {2}.'.format(
        numSubClusters, i + 1, zipcode))
    clusterDict = mainClusterDict['clusters'][i]
    clusterDict['numSubClusters'] = numSubClusters
    count = clusterDict['count']
    allListingUrls = []
    manager = Manager()
    parallelDict = manager.dict()
    jobs = []
    timeouts = []

    for j in range(len(subClusters)):
        if ('subClusters' in clusterDict.keys()) and \
           (j in clusterDict['subClusters'].keys()) and \
           (clusterDict['subClusters'][j]['complete']):
            continue
        else:
            proc = Process(
                target=scrapeSubClusterUrls,
                args=(parallelDict, mainClusterUrl, i,
                      numSubClusters, j, count, chromeOptions))
            proc.start()
            jobs.append(proc)

    for j, job in enumerate(jobs):
        job.join(timeout)
        if job.is_alive():
            job.terminate()
            timeouts.append(j)
            print('Subcluster {0}.{1} timed out. Had to terminate.'.format(i + 1, j + 1))

    clusterDict['subClusters'] = dict(parallelDict)
    for j in timeouts:
        clusterDict['subClusters'][j]['clickable'] = False
    subClustersDict = clusterDict['subClusters']
    subClustersOver350 = [j for j in subClustersDict.keys()
                          if subClustersDict[j]['count'] > 345]
    numSubClustersOver350 = len(subClustersOver350)
    subClustersNotClicked = [j for j in subClustersDict.keys()
                             if not subClustersDict[j]['clickable']]
    numSubClustersNotClicked = len(subClustersNotClicked)
    for j in subClustersDict.keys():
        allListingUrls += subClustersDict[j]['listingUrls']
    uniqueUrls = set(allListingUrls)
    pctObtained = round(len(uniqueUrls) / count, 3) * 100

    clusterDict.update(
        {'subClustersOver350': subClustersOver350,
            'numSubClustersOver350': numSubClustersOver350,
            'subClustersNotClicked': subClustersNotClicked,
            'numSubClustersNotClicked': numSubClustersNotClicked,
            'pctObtained': pctObtained,
            'listingUrls': uniqueUrls})

    return


def getMainClusters(driver, mainClusterDict, zipcode):
    zc = zipcode
    origUrl = driver.current_url
    clusters = getClusters(driver)
    numClusters = len(clusters)
    mainClusterDict.update({'numClusters': numClusters})
    count = getListingCount(driver)

    print('Found {0} clusters in zipcode {1}.'.format(numClusters, zc))
    for i in range(numClusters):
        clusterID = i + 1
        sttm = time.time()
        print('Processing cluster {0} of {1} in zipcode {2}.'.format(
            i + 1, numClusters, zc))
        if (i in mainClusterDict['clusters'].keys()) and \
           (mainClusterDict['clusters'][i]['complete']):
            print('Cluster {0} of {1} already processed with {2}% '.format(
                i + 1, numClusters, mainClusterDict['clusters'][i]['pctObtained']) +
                'of unique listings obtained.')
            continue
        else:   
            if i not in mainClusterDict['clusters'].keys():
                mainClusterDict['clusters'][i] = instantiateClusterDict()
        assert len(clusters) == numClusters
        assert driver.current_url == origUrl
        try:
            acMoveAndClick(driver, clusters[i])
            ensureMapClickable(driver, clusterID)
            if driver.current_url == origUrl:
                acMoveAndClick(driver, clusters[i])
                ensureMapClickable(driver, clusterID)
                assert driver.current_url != origUrl
            mainClusterDict['clusters'][i].update({'clickable': True})
        except:
            print('Cluster {0} from zipcode {1} could not be clicked.'.format(
                i + 1, zc))
            mainClusterDict['clusters'][i].update({'clickable': False})
            continue
        waitForListingsToLoad(driver, count)
        count = getListingCount(driver)
        mainClusterDict['clusters'][i].update({'count': count,
                                               'url': driver.current_url})
        if count > 345:
            checkForClusters(driver)
            getSubClustersInParallel(driver, mainClusterDict, i,
                                     chrome_options, zipcode)
        else:
            listingUrls = getAllUrls(driver)
            pctObtained = round(len(listingUrls) / count, 3) * 100.0
            mainClusterDict['clusters'][i].update(
                {'pctObtained': pctObtained,
                    'listingUrls': listingUrls})
        clusterInfo = mainClusterDict['clusters'][i]
        print(('{0} of {1} unique listings ({2}%) '
               'in cluster {3} from zipcode {4} were scraped.').format(
                   len(clusterInfo['listingUrls']),
                   count, clusterInfo['pctObtained'], i + 1, zc))
        if clusterInfo['numSubClustersOver350'] > 0:
            print(('{0} of {1} subclusters in cluster {2} '
                   'from zipcode {3} had more than 350 listings.').format(
                clusterInfo['numSubClustersOver350'], clusterInfo['numSubClusters'], i + 1, zc))
        if clusterInfo['numSubClustersNotClicked'] > 0:
            print(('{0} of {1} subclusters in cluster {2} '
                   'from zipcode {3} were not clicked.').format(
                clusterInfo['numSubClustersNotClicked'], clusterInfo['numSubClusters'], i + 1, zc))
        print('Back to main page for zipcode {0}.'.format(zc))
        driver.get(origUrl)
        ensureMapClickable(driver, clusterID)
        mainClusterDict['clusters'][i]['complete'] = True
        waitForListingsToLoad(driver, count)
        count = getListingCount(driver)
        clusters = getClusters(driver)
    return


def getPageUrls(driver):
    zcUrls = []
    try:
        pageLinks = driver.find_elements(By.XPATH, '''//tbody[@class="tableList"]/tr/
            td[@class="column column_1 col_address"]/div/a''')
        for pageLink in pageLinks:
            url = pageLink.get_attribute('href')
            zcUrls.append(url)
    except:
        pass
    return zcUrls


def getAllUrls(driver):

    allUrls = []
    firstUrls = getPageUrls(driver)
    if len(firstUrls) == 0:
        return allUrls
    allUrls += firstUrls
    nextPages = driver.find_elements(
        By.XPATH, '''//*[@id="sidepane-root"]/div[2]/''' +
        '''div/div[4]/div[2]/a[contains(@class,"goToPage")]''')
    if len(nextPages) == 0:
        return allUrls
    actions = ActionChains(driver)
    for k in range(1, len(nextPages)):
        checkForLoginPrompt(driver)
        nextPage = driver.find_element(
            By.XPATH,
            '//a[@data-rf-test-id="react-data-paginate-page-{0}"]'.format(k))
        actions.move_to_element(nextPage).perform()
        nextPage.click()
        time.sleep(2)
        nextUrls = getPageUrls(driver)
        allUrls += nextUrls
    return allUrls


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


def pickleClusterDict(clusterDict, zipcode):
    pickle.dump(clusterDict, open(
        'data/pickles/main_cluster_dict_{0}.pkl'.format(zipcode), 'wb'))


def writeCsv(outfile, row):
    with open(outfile, 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(row)


def writeDb(eventList):
    dbname = 'redfin'
    host = 'localhost'
    port = 5432
    conn_str = "dbname={0} host={1} port={2}".format(dbname, host, port)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    num_listings = len(eventList)
    prob_PIDs = []
    dupes = []
    writes = []
    for i, row in eventList:
        try:
            cur.execute('''INSERT INTO sales_listings
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                            (row['pid'],row['date'].to_datetime(),row['region'],row['neighborhood'],
                            row['rent'],row['bedrooms'],row['sqft'],row['rent_sqft'],
                            row['longitude'],row['latitude'],row['county'],
                            row['fips_block'],row['state'],row['bathrooms']))
            conn.commit()
            writes.append(row['pid'])
        except Exception, e:
            if 'duplicate key value violates unique' in str(e):
                dupes.append(row['pid'])
            else:
                prob_PIDs.append(row['pid'])
            conn.rollback()
    cur.close()
    conn.close()
    return prob_PIDs, dupes, writes


newNotListed = []

for zc in zips:
    allZipCodeUrls = []
    print("Getting driver for zipcode {0}.".format(zc))
    if zc in not_listed:
        continue
    driver = goToRedfin(zc, chrome_options)
    print("Got driver for zipcode {0}.".format(zc))
    if driver.current_url == 'https://www.redfin.com/out-of-area-signup':
        print('No landing page for zipcode {0}.'.format(zc))
        newNotListed.append(zc)
        continue
    totalListings = getListingCount(driver)
    print('Found {0} listings in zipcode {1}.'.format(totalListings, zc))
    sttm = time.time()
    mainClusterDict = instantiateMainClusterDict(zc)
    if totalListings < 345:
        uniqueUrls = getAllUrls(driver)
        numMainClusters = 0
        clustersNotClicked = 0
    else:
        checkForClusters(driver)
        getMainClusters(driver, mainClusterDict, zc)
        numMainClusters = mainClusterDict['numClusters']
        clustersDict = mainClusterDict['clusters']
        clustersNotClicked = [i for i in clustersDict.keys()
                              if not clustersDict[i]['clickable']]
        numClustersNotClicked = len(clustersNotClicked)
        for i in clustersDict.keys():
            allZipCodeUrls += clustersDict[i]['listingUrls']
        uniqueUrls = set(allZipCodeUrls)
    totalTime = time.time() - sttm
    pctObtained = round(len(uniqueUrls) / totalListings, 3) * 100.0
    print(('{0} of {1} of unique listings ({2}%) from zipcode '
          '{3} were scraped.').format(
          len(uniqueUrls), totalListings, pctObtained, zc))
    print('Took {0} seconds to process zipcode {1}.'.format(totalTime, zc))
    if numClustersNotClicked > 0:
        print(('{0} of {1} main clusters from zipcode '
              '{2} were not clicked.').format(
              numClustersNotClicked, numMainClusters, zc))
    





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
