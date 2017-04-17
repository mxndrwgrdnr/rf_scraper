from __future__ import division
from pyvirtualdisplay import Display
import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions \
    import WebDriverException, NoSuchElementException, TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from itertools import izip_longest
from datetime import datetime as dt
import logging
import time
import pickle
from multiprocessing import Process, Manager


chrome_options = Options()
chrome_options.add_extension("./proxy.zip")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--window-size=1024,768")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-infobars")
sttm = dt.now().strftime('%Y%m%d-%H%M%S')
dataDir = './data/'


class redfinScraper(object):

    def __init__(self, virtualDisplay=False, chromeOptions=chrome_options,
                 startTime=sttm, dataDir=dataDir):

        self.chromeOptions = chromeOptions
        self.startTime = startTime
        self.dataDir = dataDir
        if virtualDisplay:
            display = Display(visible=0, size=(1024, 768))
            display.start()
        log_fname = './logs/scrape_' + self.startTime + '.log'
        print('Writing log to {0}'.format(log_fname))
        logging.basicConfig(filename=log_fname, level=logging.INFO)

    def getChromeDriver(self):
        try:
            driver = webdriver.Chrome(chrome_options=self.chromeOptions)
        except WebDriverException:
            return False
        return driver

    def switchToTableView(self, driver):
        try:
            button = driver.find_element(
                By.XPATH, '''//span[@data-rf-test-name="tableOption"]''')
        except NoSuchElementException:
            return
        try:
            button.click()
            return
        except WebDriverException:
            logging.info('Could not switch to redfin table view.')
            return

    def checkForLoginPrompt(self, driver, zipOrClusterId=False):
        try:
            loginPrompt = driver.find_element(
                By.XPATH, '//div[@data-rf-test-name="dialog-close-button"]')
            logging.info(
                'Detected login prompt at {0}. Will try to close.'.format(
                    zipOrClusterId))
        except NoSuchElementException:
            return False
        loginPrompt.click()
        return True

    def checkForPopUp(self, driver):
        try:
            popup = driver.find_element(
                By.XPATH,
                '//a[@href="https://www.redfin.com' +
                '/buy-a-home/classes-and-events"]')
            popup.find_element(By.XPATH, '../../../img').click()
            return
        except NoSuchElementException:
            return

    def checkForMap(self, driver, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = driver.current_url
        noMap = True
        sttm = time.time()
        tries = 1
        while noMap and time.time() - sttm < 60:
            try:
                driver.find_element(
                    By.XPATH, '//div[@class="GoogleMapView"]')
                noMap = False
            except NoSuchElementException:
                logging.info(
                    'No map detected at {0}. Refreshing browser. Try #{1}'.
                    format(zipOrClusterId, tries))
                driver.refresh()
                tries += 1
                time.sleep(5)
        if noMap:
            logging.info('Could not load map at {0}'.format(zipOrClusterId))
            return False
        else:
            if tries > 1:
                logging.info(
                    'Found map after all at {0}.'.format(zipOrClusterId))
            return True

    def waitForProgressBar(self, driver, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = driver.current_url
        try:
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                        '//div[@data-rf-test-name="progress-bar-text"]')))
        except TimeoutException:
            mapFound = self.checkForMap(driver, zipOrClusterId)
            return mapFound
        try:
            WebDriverWait(driver, 60).until(
                EC.invisibility_of_element_located(
                    (By.XPATH,
                        '//div[@data-rf-test-name="progress-bar-text"]')))
        except TimeoutException:
            logging.info(
                'Timed out waiting for progress bar to finish' +
                ' at {0}. Refreshing browser.'.format(zipOrClusterId))
            driver.refresh()
            return False
        return True

    def checkForFlyout(self, driver, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = driver.current_url
        flyOut = True
        try:
            selectedRow = driver.find_element(
                By.XPATH, '//tr[@class="selected tableRow"]')
        except NoSuchElementException:
            logging.info(
                'No rows detected at {0}. Refreshing browser.'.format(
                    zipOrClusterId))
            driver.refresh()
            return
        selectedRowNum = int(selectedRow.get_attribute('id').split('_')[1])
        while flyOut:
            try:
                driver.find_element(
                    By.XPATH, '//div[@class="clickableHome MultiUnitFlyout"]')
                logging.info(
                    'Flyout menu detected at {0}.' +
                    ' Clicking next row to close flyout.'.format(
                        zipOrClusterId))
                nextRow = driver.find_element(
                    By.XPATH,
                    '//tr[@id="ReactDataTableRow_{0}"]'.format(
                        selectedRowNum + 1))
                actions = ActionChains(driver)
                actions.move_to_element(nextRow).perform()
                nextRow.click()
                logging.info('Flyout should be gone at {0}.'.format(
                    zipOrClusterId))
                self.checkForLoginPrompt(driver)
                selectedRowNum += 1
            except NoSuchElementException:
                flyOut = False
        return

    def ensureMapClickable(self, driver, zipOrClusterId=False):
        ok = self.ensurePageScrapable(driver, zipOrClusterId)
        self.checkForPopUp(driver)
        self.checkForFlyout(driver, zipOrClusterId)
        return ok

    def ensurePageScrapable(self, driver, zipOrClusterId=False):
        self.checkForLoginPrompt(driver, zipOrClusterId)
        ok = self.waitForProgressBar(driver, zipOrClusterId)
        self.switchToTableView(driver)
        return ok

    def acMoveAndClick(self, driver, origUrl, element, clusterId):
        actions = ActionChains(driver)
        actions.move_to_element(element)
        try:
            actions.click(element)
            actions.perform()
        except WebDriverException:
            logging.info('(sub)cluster {0} could not be clicked on first try.'.
                         format(clusterId))
        self.ensurePageScrapable(driver, clusterId)
        sttm = time.time()
        while (driver.current_url == origUrl) and (time.time() - sttm < 15):
            time.sleep(1)

    def clickIfClickable(self, driver, origUrl, element, clusterId):
        self.acMoveAndClick(driver, origUrl, element, clusterId)
        if driver.current_url == origUrl:
            self.acMoveAndClick(driver, origUrl, element, clusterId)
        if driver.current_url == origUrl:
            return False
        else:
            return True

    def goToRedfin(self, zipcode):
        driver = self.getChromeDriver()
        zc = zipcode
        url = "http://www.redfin.com/zipcode/" + zc + \
            "/filter/include=sold-all"
        driver.get(url)
        self.switchToTableView(driver)
        self.ensureMapClickable(driver)
        return driver

    def goToRedfinViewport(self, url, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = url
        driver = self.getChromeDriver()
        if not driver:
            return False
        driver.get(url)
        self.switchToTableView(driver)
        if self.ensureMapClickable(driver, zipOrClusterId):
            return driver
        else:
            logging.info(
                'Map was not clickable at {0}: {1}'.format(
                    zipOrClusterId, url))
            driver.quit()
            return False

    def waitForListingsToLoad(self, driver, count):
        newCount = self.getListingCount(driver)
        sttm = time.time()
        while (newCount == count) and (time.time() - sttm < 30):
            newCount = self.getListingCount(driver)

    def getClusters(self, driver):
        clusters = driver.find_elements(
            By.XPATH, '//div[@class="numHomes"]')
        return clusters

    def checkForClusters(self, driver):
        if not len(self.getClusters(driver)):
            driver.refresh()

    def getListingCount(self, driver):
        elemText = driver.find_elements(
            By.XPATH, '//div[@class="homes summary"]')[0].text
        if 'of' in elemText:
            countStr = elemText.split('of')[1].split('Homes')[0].strip()
        else:
            countStr = elemText.split()[1]
        assert countStr.isdigit()
        return int(countStr)

    def instantiateMainClusterDict(self, zipcode):
        try:
            mcd = pickle.load(
                open(self.dataDir + 'pickles/' +
                     'main_cluster_dict_{0}.pkl'.format(zipcode), 'rb'))
            logging.info('Loaded existing zipcode cluster data.')
        except IOError:
            mcd = {'clusters': {}, 'numClusters': 0,
                   'numClustersNotClicked': 0, 'clustersNotClicked': [],
                   'listingUrls': []}
        return mcd

    def instantiateClusterDict(self):
        clusterDict = {'complete': False,
                       'count': 0,
                       'numSubClusters': 0,
                       'numSubClustersNotClicked': 0,
                       'subClustersOver350': [],
                       'numSubClustersOver350': 0,
                       'subClustersNotClicked': [],
                       'listingUrls': []}
        return clusterDict

    def formatSubClusterDict(self, complete, url, clickable,
                             count, listingUrls):
        clusterDict = {'complete': complete,
                       'url': url,
                       'clickable': clickable,
                       'count': count,
                       'listingUrls': listingUrls}
        return clusterDict

    def scrapeSubClusterUrls(self, parallelDict, mainClusterUrl, mainClusterNo,
                             numSubClusters, subClusterNo, mainClusterCount):
        i = mainClusterNo
        j = subClusterNo
        subClusterID = '{0}.{1}'.format(i + 1, j + 1)
        complete = False
        url = None
        clickable = None
        count = 0
        listingUrls = []
        scDriver = self.goToRedfinViewport(
            mainClusterUrl, 'main cluster {0} for subcluster {1}'.format(
                i + 1, j + 1))
        if not scDriver:
            logging.info(
                'Subcluster {0} Chrome instance failed to load.'.format(
                    subClusterID))
            parallelDict[j] = self.formatSubClusterDict(
                complete, url, clickable, count, listingUrls)
            return
        subClusters = self.getClusters(scDriver)
        assert len(subClusters) == numSubClusters
        assert scDriver.current_url == mainClusterUrl
        clickable = self.clickIfClickable(
            scDriver, mainClusterUrl, subClusters[j], subClusterID)
        if clickable is False:
            logging.info(
                'Subcluster {0} could not be clicked.'.format(
                    subClusterID))
            complete = True
            scDriver.quit()
            parallelDict[j] = self.formatSubClusterDict(
                complete, url, clickable, count, listingUrls)
            return
        self.waitForListingsToLoad(scDriver, mainClusterCount)
        count = self.getListingCount(scDriver)
        url = scDriver.current_url
        if count > 345:
            logging.info(
                'Subcluster {0} had more than 350 listings.'.format(
                    subClusterID))
        else:
            listingUrls = self.getAllUrls(scDriver)
            pctObtained = round(len(listingUrls) / count, 2) * 100
            logging.info(
                'Subcluster {3} returned {0} of {1} ({2}%) '.
                format(len(listingUrls), count, pctObtained, subClusterID) +
                'listing urls.')
        complete = True
        parallelDict[j] = self.formatSubClusterDict(
            complete, url, clickable, count, listingUrls)
        scDriver.quit()
        return

    def getSubClustersInParallel(self, driver, mainClusterDict, mainClusterNo,
                                 zipcode, timeout=120):
        i = mainClusterNo
        mainClusterUrl = driver.current_url
        subClusters = self.getClusters(driver)
        numSubClusters = len(subClusters)
        logging.info(
            'Found {0} subclusters in cluster {1} in zipcode {2}.'.format(
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
                    target=self.scrapeSubClusterUrls,
                    args=(parallelDict, mainClusterUrl, i,
                          numSubClusters, j, count))
                proc.start()
                jobs.append(proc)

        for j, job in enumerate(jobs):
            job.join(timeout)
            if job.is_alive():
                job.terminate()
                timeouts.append(j)
                logging.info(
                    'Subcluster {0}.{1} timed out. Had to terminate.'.format(
                        i + 1, j + 1))

        clusterDict['subClusters'] = dict(parallelDict)
        for j in timeouts:
            clusterDict['subClusters'][j] = self.formatSubClusterDict(
                False, None, False, None, None)
        subClustersDict = clusterDict['subClusters']
        subClustersOver350 = [j for j in subClustersDict.keys()
                              if subClustersDict[j]['count'] > 345]
        numSubClustersOver350 = len(subClustersOver350)
        subClustersNotClicked = [j for j in subClustersDict.keys()
                                 if not subClustersDict[j]['clickable']]
        numSubClustersNotClicked = len(subClustersNotClicked)
        for j in subClustersDict.keys():
            if subClustersDict[j]['listingUrls'] is None:
                logging.info(
                    'Subcluster {0}.{1} returned no listing urls'.format(
                        i + 1, j + 1))
            else:
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

    def getMainClusters(self, driver, mainClusterDict, zipcode):
        zc = zipcode
        origUrl = driver.current_url
        clusters = self.getClusters(driver)
        numClusters = len(clusters)
        mainClusterDict.update({'numClusters': numClusters})
        count = self.getListingCount(driver)

        logging.info(
            'Found {0} clusters in zipcode {1}.'.format(numClusters, zc))
        for i in [3]:  # range(numClusters):
            clusterID = i + 1
            logging.info(
                'Processing cluster {0} of {1} in zipcode {2}.'.format(
                    i + 1, numClusters, zc))
            if (i in mainClusterDict['clusters'].keys()) and \
               (mainClusterDict['clusters'][i]['complete']):
                logging.info(
                    'Cluster {0} of {1} already processed with {2}% '.format(
                        i + 1, numClusters,
                        mainClusterDict['clusters'][i]['pctObtained']) +
                    'of unique listings obtained.')
                continue
            else:
                if i not in mainClusterDict['clusters'].keys():
                    mainClusterDict['clusters'][i] = \
                        self.instantiateClusterDict()
            assert len(clusters) == numClusters
            assert driver.current_url == origUrl
            clickable = self.clickIfClickable(
                driver, origUrl, clusters[i], clusterID)
            mainClusterDict['clusters'][i].update({'clickable': clickable})
            if clickable is False:
                logging.info(
                    'Main cluster {0} from zipcode {1} ' +
                    'could not be clicked.'.format(
                        i + 1, zc))
                continue
            self.waitForListingsToLoad(driver, count)
            count = self.getListingCount(driver)
            mainClusterDict['clusters'][i].\
                update({'count': count, 'url': driver.current_url})
            if count > 345:
                self.checkForClusters(driver)
                self.getSubClustersInParallel(
                    driver, mainClusterDict, i, zipcode)
            else:
                listingUrls = self.getAllUrls(driver)
                pctObtained = round(len(listingUrls) / count, 3) * 100.0
                mainClusterDict['clusters'][i].update(
                    {'pctObtained': pctObtained,
                        'listingUrls': listingUrls})
            clusterInfo = mainClusterDict['clusters'][i]
            logging.info('{0} of {1} unique listings ({2}%) '.format(
                len(clusterInfo['listingUrls']), count,
                clusterInfo['pctObtained']) +
                'in cluster {0} from zipcode {1} were scraped.'.format(
                    i + 1, zc))
            if clusterInfo['numSubClustersOver350'] > 0:
                logging.info('{0} of {1} subclusters in cluster {2} '.format(
                    clusterInfo['numSubClustersOver350'],
                    clusterInfo['numSubClusters'], i + 1) +
                    'from zipcode {0} had more than 350 listings.'.format(zc))
            if clusterInfo['numSubClustersNotClicked'] > 0:
                logging.info('{0} of {1} subclusters in cluster {2} '.format(
                    clusterInfo['numSubClustersNotClicked'],
                    clusterInfo['numSubClusters'], i + 1) +
                    'from zipcode {0} were not clicked.'.format(zc))
            logging.info('Back to main page for zipcode {0}.'.format(zc))
            driver.get(origUrl)
            self.ensureMapClickable(driver, clusterID)
            mainClusterDict['clusters'][i]['complete'] = True
            self.waitForListingsToLoad(driver, count)
            count = self.getListingCount(driver)
            clusters = self.getClusters(driver)
        return

    def getPageUrls(self, driver):
        zcUrls = []
        try:
            pageLinks = driver.find_elements(By.XPATH, '''//tbody[@class="tableList"]/tr/
                td[@class="column column_1 col_address"]/div/a''')
            for pageLink in pageLinks:
                url = pageLink.get_attribute('href')
                zcUrls.append(url)
        except Exception, e:
            print(e)
            pass
        return zcUrls

    def getAllUrls(self, driver):
        allUrls = []
        firstUrls = self.getPageUrls(driver)
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
            self.checkForLoginPrompt(driver)
            nextPage = driver.find_element(
                By.XPATH,
                '//a[@data-rf-test-id="react-data-paginate-page-{0}"]'.format(
                    k))
            actions.move_to_element(nextPage).perform()
            try:
                nextPage.click()
            except WebDriverException:
                continue
            time.sleep(2)
            nextUrls = self.getPageUrls(driver)
            allUrls += nextUrls
        return allUrls

    def getUrlsByZipCode(self, zipcodes, notListed):
        newNotListed = []
        for zc in zipcodes:
            allZipCodeUrls = []
            logging.info("Getting driver for zipcode {0}.".format(zc))
            if zc in notListed:
                continue
            driver = self.goToRedfin(zc)
            logging.info("Got driver for zipcode {0}.".format(zc))
            if driver.current_url == \
                    'https://www.redfin.com/out-of-area-signup':
                logging.info('No landing page for zipcode {0}.'.format(zc))
                newNotListed.append(zc)
                continue
            totalListings = self.getListingCount(driver)
            logging.info(
                'Found {0} listings in zipcode {1}.'.format(totalListings, zc))
            sttm = time.time()
            mainClusterDict = self.instantiateMainClusterDict(zc)
            if totalListings < 345:
                uniqueUrls = self.getAllUrls(driver)
                numMainClusters = 0
                clustersNotClicked = 0
            else:
                self.checkForClusters(driver)
                self.getMainClusters(driver, mainClusterDict, zc)
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
            logging.info(
                '{0} of {1} of unique listings ({2}%) from zipcode '.format(
                    len(uniqueUrls), totalListings, pctObtained) +
                '{0} were scraped.'.format(zc))
            logging.info('Took {0} seconds to process zipcode {1}.'.format(
                totalTime, zc))
            if numClustersNotClicked > 0:
                print('{0} of {1} main clusters from zipcode '.format(
                    numClustersNotClicked, numMainClusters) +
                    '{0} were not clicked.'.format(zc))
        return mainClusterDict, newNotListed

    def getEventDate(self, htmlEventElement):
        rawDateStr = htmlEventElement.find_element(
            By.XPATH, './/td[contains(@class,"date-col")]').text
        dateStr = dt.strptime(rawDateStr, '%b %d, %Y').strftime('%Y-%m-%d')
        return dateStr

    def getEventPrice(self, htmlEventElement):
        price = htmlEventElement.find_element(
            By.XPATH, './/td[contains(@class,"price-col")]') \
            .text.strip('$').replace(',', '')
        return price.encode('utf-8')

    def getEventsFromListing(self, driver):
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
        except NoSuchElementException:
            beds = None
        try:
            bathsInfo = info.find_element(
                By.XPATH, '''.//span[contains(text(),"Bath")]/..''') \
                .text.split('\n')
            baths = bathsInfo[0].encode('utf-8')
        except NoSuchElementException:
            baths = None
        try:
            sqftInfo = info.find_element(
                By.XPATH, '''.//span[@class="sqft-label"]/..
                /span[@class="main-font statsValue"]''').text
            sqft = sqftInfo.replace(',', '').encode('utf-8')
        except NoSuchElementException:
            sqft = None
        try:
            yearBuiltInfo = info.find_element(
                By.XPATH, '''.//span[text()="Built: "]/..''').text.split(': ')
            yearBuilt = yearBuiltInfo[1].encode('utf-8')
        except NoSuchElementException:
            yearBuilt = None
        try:
            keyDetails = driver.find_element(
                By.XPATH, '''//div[@class="keyDetailsList"]
                /div/span[text()="MLS#"]/..''').text.split('\n')
            if keyDetails[0] == "MLS#":
                mls = keyDetails[1].encode('utf-8')
            else:
                mls = None
        except NoSuchElementException:
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
            logging.info('Working on row {0}'.format(i))
            if 'Sold' in historyRow.text:
                curSaleDt = self.getEventDate(historyRow)
                curSalePrice = self.getEventPrice(historyRow)
                if lastEvent == 'sale' and curSaleDt != saleDt:
                    events.append(
                        staticAttrs + [saleDt, salePrice, None, None])
                if i + 1 == len(historyRows):
                    events.append(
                        staticAttrs + [curSaleDt, curSalePrice, None, None])
                lastEvent = 'sale'
                saleDt, salePrice = curSaleDt, curSalePrice
            elif 'Listed' in historyRow.text and lastEvent == 'sale':
                listDt = self.getEventDate(historyRow)
                listPrice = self.getEventPrice(historyRow)
                events.append(
                    staticAttrs + [saleDt, salePrice, listDt, listPrice])
                lastEvent = 'listing'
            else:
                continue

        return events

    def pickleClusterDict(self, clusterDict, zipcode):
        pickle.dump(clusterDict, open(
            'data/pickles/main_cluster_dict_{0}.pkl'.format(zipcode), 'wb'))

    def writeCsv(self, outfile, row):
        with open(outfile, 'wb') as f:
            writer = csv.writer(f)
            writer.writerow(row)

    def writeDb(self, eventList):
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








        # with open("processed_urls.csv", 'r') as pus:
        #     urls = pus.read().split('\r\n')

        # with open('processed_urls.csv', 'a') as pus:
        #     pus_writer = csv.writer(pus)
        #     with open(fname, 'wb') as f:
        #         writer = csv.writer(f)
        #         for i, url in enumerate(allZipCodeUrls):
        #             if url in urls:
        #                 logging.info('been there done that.')
        #                 continue
        #             logging.info('Scraping events for listing {0} of {1}'.format(
        #                 i + 1, len(allZipCodeUrls)))
        #             try:
        #                 driver.get(url)
        #                 events = getEventsFromListing(driver)
        #                 for j, event in enumerate(events):
        #                     logging.info("writing event {0} of {1}".format(
        #                         j + 1, len(events)))
        #                     writer.writerow(event)
        #             except Exception, e:
        #                 logging.info(Exception, e, url)
        #                 break
        #                 continue
        #             pus_writer.writerow([url])
        #         break


