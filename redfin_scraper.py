import csv
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.chrome.options import Options
from pyzipcode import ZipCodeDatabase
from itertools import izip_longest
from datetime import datetime as dt
import pandas as pd
import time

display = Display(visible=0, size=(800, 600))
display.start()

chrome_options = Options()
chrome_options.add_extension("/home/mgardner/rf_scraper/proxy.zip")
chrome_options.add_argument('--ignore-certificate-errors')
chrome_options.add_argument("--start-maximized")

OUT_DIR = '/home/mgardner/rf_scraper/data/'
zcdb = ZipCodeDatabase()
zips = [zc.zip for zc in zcdb.find_zip()]
startDate = dt.now().strftime('%Y-%m-%d')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]


def getClusterUrls(clustersElems):
    clusters = clustersElems
    clusterUrlsUnder350 = []
    clusterUrlsOver350 = []
    count = getListingCount(driver)

    for i in range(len(clusters)):
        print(str(i) + ' of ' + str(len(clusters)))
        try:
            clusters[i].click()
        except:
            continue
        newCount = getListingCount(driver)
        while newCount == count:
            newCount = getListingCount(driver)
        if getListingCount(driver) > 350:
            clusterUrlsOver350.append(driver.current_url)
        else:
            clusterUrlsUnder350.append(driver.current_url)
        driver.back()
        newCount = getListingCount(driver)
        while newCount == count:
            newCount = getListingCount(driver)
        count = getListingCount(driver)
        clusters = driver.find_elements(By.XPATH, '//div[@class="clusterMarkersContainer"]/div')

    # for url in clusterUrlsOver350:
    #     driver.get(url)
    #     subCount = getListingCount(driver)
    #     try:
    #         subClusters = driver.find_elements(By.XPATH, '//div[@class="clusterMarkersContainer"]/div')
    #     except:
    #         clusterUrlsUnder350.append(url)
    #         continue
    #     for j in range(len(subClusters)):
    #         try:
    #             subClusters[j].click()
    #         except:
    #             continue
    #         newSubCount = getListingCount(driver)
    #         while newSubCount == subCount:
    #             newSubCount = getListingCount(driver)
    #         clusterUrlsUnder350.append(driver.current_url)
    #         driver.back()
    #         newSubCount = getListingCount(driver)
    #         while newSubCount == subCount:
    #             newSubCount = getListingCount(driver)
    #         subCount = getListingCount(driver)
    #         subClusters = driver.find_elements(By.XPATH, '//div[@class="clusterMarkersContainer"]/div')

    return clusterUrlsUnder350, clusterUrlsOver350


def getChromeDriver(zipcode, chromeOptions=None):
    driver = webdriver.Chrome(chrome_options=chromeOptions)
    driver.implicitly_wait(10)
    zc = zipcode
    url = "http://www.redfin.com/zipcode/" + zc + \
        "/filter/include=sold-all"
    driver.get(url)
    return driver


def getClusters(driver):
    clusters = driver.find_elements(
        By.XPATH, '//div[@class="clusterMarkerImage"]')
    return clusters


def getListingCount(driver):
    elemText = driver.find_elements(
        By.XPATH, '//div[@class="homes summary"]')[0].text
    if 'of' in elemText:
        countStr = elemText.split('of')[1].split('Homes')[0].strip()
    else:
        countStr = elemText.split()[0]
    return int(countStr)


def getPageUrls(driver):

    zcUrls = []
    try:
        pageLinks = driver.find_elements(By.XPATH, '''//tbody[@class="tableList"]/tr/
            td[@class="column column_1 col_address"]/div/a''')
        for pageLink in pageLinks:
            url = pageLink.get_attribute('href')
            zcUrls.append(url)
    except:
        print("no page links for " + zc)

    return zcUrls


def getAllUrls(driver):

    allZipCodeUrls = []

    firstUrls = getPageUrls(driver)
    if len(firstUrls) == 0:
        print("no listings for " + zc)
        noListings.append(zc)

    allZipCodeUrls += firstUrls
    nextPages = driver.find_elements(
        By.XPATH, '//a[@class="clickable goToPage"]')

    for page in nextPages:
        page.click()
        nextUrls = getPageUrls(driver)
        allZipCodeUrls += nextUrls

    return allZipCodeUrls


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


for zc in ['94609']:
# for zc in zips:
    fname = OUT_DIR + 'sales_' + zc + '_' + startDate + '.csv'
    print("getting driver for " + zc)
    if zc in not_listed:
        continue

    allZipCodeUrls = []
    driver = getChromeDriver(zc, chromeOptions=chrome_options)
    print("got driver for " + zc)

    if driver.current_url == 'https://www.redfin.com/out-of-area-signup':
        print('no landing page for ' + zc)
        newNotListed.append(zc)
        continue

    totalListings = getListingCount(driver)

    break

    if totalListings <= 350:
        allZipCodeUrls = getAllUrls(driver)

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
