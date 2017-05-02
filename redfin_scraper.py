from __future__ import division
from pyvirtualdisplay import Display
import csv
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions \
    import WebDriverException, NoSuchElementException, TimeoutException, \
    StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from itertools import izip_longest
from datetime import datetime as dt, timedelta as td
import logging
import time
import pickle
from Queue import Empty
from multiprocess import Process, Manager, Queue
import psycopg2
from psycopg2 import IntegrityError, InterfaceError
import errno
from socket import error as socket_error
import os
os.environ["DBUS_SESSION_BUS_ADDRESS"] = '/dev/null'


class redfinScraper(object):

    def __init__(
            self, eventFile, processedUrlsFName, dataDir, startTime,
            virtualDisplay=False, subClusterMode='series',
            eventMode='series', timeFilter='sold-all', chromeOptions=None):

        self.dataDir = dataDir
        self.eventFile = self.dataDir + eventFile
        self.processedUrlsFName = self.dataDir + processedUrlsFName
        self.chromeOptions = chromeOptions
        self.startTime = startTime
        self.timeFilter = timeFilter
        self.subClusterMode = subClusterMode
        self.eventMode = eventMode
        if virtualDisplay:
            display = Display(visible=0, size=(1024, 768))
            display.start()
        log_fname = './logs/scrape_' + self.startTime + '.log'
        print('Writing log to {0}'.format(log_fname))
        logging.basicConfig(filename=log_fname, level=logging.INFO)
        self.notListedFName = './not_listed.csv'
        self.zipsReqSignInFName = './zips_req_signin.csv'
        with open(self.notListedFName, 'rb') as f:
            reader = csv.reader(f)
            self.not_listed = [zc for zclist in reader for zc in zclist]
        self.mainDriver = None
        self.mainClusterDict = None
        self.listingUrls = None
        self.pctUrlsScraped = None
        self.pctUrlsWithEvents = None
        self.pctEventsWritten = None
        self.eventList = None

    def getChromeDriver(self):
        try:
            driver = webdriver.Chrome(chrome_options=self.chromeOptions)
        except WebDriverException as e:
            return False, str(e)
        except socket_error as serr:
            if serr.errno != errno.ECONNREFUSED:
                raise serr
            else:
                try:
                    driver = webdriver.Chrome(
                        chrome_options=self.chromeOptions)
                except WebDriverException as e:
                    return False, str(e)
        except Exception as e:
            return False, str(e)
        return driver, 'ok'

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
                try:
                    driver.refresh()
                except TimeoutException:
                    msg = 'Timed out loading map at {0}'.format(zipOrClusterId)
                    return False, msg
                tries += 1
                time.sleep(5)
        if noMap:
            msg = 'Could not load map at {0}'.format(zipOrClusterId)
            return False, msg
        else:
            msg = 'Found map for {0} after {1} tries.'.format(
                zipOrClusterId, tries)
            return True, msg

    def waitForProgressBar(self, driver, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = driver.current_url
        try:
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.XPATH,
                        '//div[@data-rf-test-name="progress-bar-text"]')))
        except TimeoutException:
            mapFound, msg = self.checkForMap(driver, zipOrClusterId)
            return mapFound, msg
        try:
            WebDriverWait(driver, 60).until(
                EC.invisibility_of_element_located(
                    (By.XPATH,
                        '//div[@data-rf-test-name="progress-bar-text"]')))
        except TimeoutException:
            msg = 'Timed out waiting for progress bar to finish' + \
                ' at {0}. Refreshing browser.'.format(zipOrClusterId)
            driver.refresh()
            return False, msg
        return True, 'ok'

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
                    'Flyout menu detected at {0}.'
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
        ok, msg = self.ensurePageScrapable(driver, zipOrClusterId)
        self.checkForPopUp(driver)
        self.checkForFlyout(driver, zipOrClusterId)
        return ok, msg

    def ensurePageScrapable(self, driver, zipOrClusterId=False):
        self.checkForLoginPrompt(driver, zipOrClusterId)
        ok, msg = self.waitForProgressBar(driver, zipOrClusterId)
        self.switchToTableView(driver)
        return ok, msg

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
        driver, msg = self.getChromeDriver()
        if not driver:
            return False, msg
        zc = zipcode
        url = "http://www.redfin.com/zipcode/" + zc + \
            "/filter/include=" + self.timeFilter
        driver.get(url)
        self.switchToTableView(driver)
        if driver.current_url == \
           'https://www.redfin.com/out-of-area-signup':
            with open(self.notListedFName, 'a') as f:
                writer = csv.writer(f)
                writer.writerow([zc])
            driver.quit()
            return False, 'Zipcode {0} out of area'.format(zc)
        mapClickable, msg = self.ensureMapClickable(driver)
        if not mapClickable:
            return False, msg
            driver.quit()
        return driver, 'ok'

    def goToRedfinViewport(self, url, zipOrClusterId=False):
        if not zipOrClusterId:
            zipOrClusterId = url
        scDriver, msg = self.getChromeDriver()
        if not scDriver:
            return False, msg
        scDriver.get(url)
        self.switchToTableView(scDriver)
        mapClickable, msg = self.ensureMapClickable(scDriver, zipOrClusterId)
        if not mapClickable:
            scDriver.quit()
            return False, msg
        else:
            return scDriver, 'ok'

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

    def closeExtraTabs(self, driver):
        if len(driver.window_handles) > 1:
            mainWindow = driver.window_handles[0]
            for window in driver.window_handles[1:]:
                driver.switch_to.window(window_name=window)
                driver.close()
                driver.switch_to.window(window_name=mainWindow)
        assert len(driver.window_handles) == 1

    def getFeaturedListingUrl(self, driver):
        linkElem = driver.find_element(
            By.XPATH, '//div[@id="listing-preview"]//a[@class="link"]')
        url = linkElem.get_attribute('href')
        return url

    def getListingCount(self, driver):
        elemText = driver.find_elements(
            By.XPATH, '//div[@class="homes summary"]')[0].text
        if 'of' in elemText:
            countStr = elemText.split('of')[1].split('Homes')[0].strip()
        elif 'Showing' in elemText:
            countStr = elemText.split()[1]
        elif len(elemText) == 2:
            countStr = elemText.split()[0]
        else:
            return 0
        assert countStr.isdigit()
        return int(countStr)

    def instantiateMainClusterDict(self, zipcode):
        try:
            mcd = pickle.load(
                open(self.dataDir + '/pickles' +
                     '/main_cluster_dict_{0}.pkl'.format(zipcode), 'rb'))
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

    def scrapeSubClusterUrls(self, IODict, mainClusterUrl, mainClusterNo,
                             numSubClusters, subClusterNo, mainClusterCount):
        i = mainClusterNo
        j = subClusterNo
        subClusterID = '{0}.{1}'.format(i + 1, j + 1)
        complete = False
        url = None
        clickable = None
        count = 0
        listingUrls = []
        scDriver, msg = self.goToRedfinViewport(
            mainClusterUrl, 'main cluster {0} for subcluster {1}'.format(
                i + 1, j + 1))
        if not scDriver:
            logging.info(
                'Subcluster {0} Chrome instance failed to load. {1}'.format(
                    subClusterID, msg))
            IODict[j] = self.formatSubClusterDict(
                complete, url, clickable, count, listingUrls)
            return
        subClusters = self.getClusters(scDriver)
        if len(subClusters) != numSubClusters:
            logging.info(
                'Got {0} subclusters when {1} were expected. Checking'
                ' for extra tabs and closing any.'.format(
                    len(subClusters), numSubClusters))
            self.closeExtraTabs(scDriver)
            subClusters = self.getClusters(scDriver)
            if len(subClusters) != numSubClusters:
                logging.info(
                    'Still have got the wrong number of clusters.'
                    ' Refreshing the browser.')
                scDriver.refresh()
            subClusters = self.getClusters(scDriver)
            if len(subClusters) != numSubClusters:
                logging.info(
                    'Subcluster {0} from could not be clicked.'.
                    format(subClusterID))
                scDriver.quit()
                IODict[j] = self.formatSubClusterDict(
                    complete, url, clickable, count, listingUrls)
                return
        clickable = self.clickIfClickable(
            scDriver, mainClusterUrl, subClusters[j], subClusterID)
        if clickable is False:
            logging.info(
                'Subcluster {0} could not be clicked.'.format(
                    subClusterID))
            complete = True
            scDriver.quit()
            IODict[j] = self.formatSubClusterDict(
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
                'Subcluster {3} returned {0} of {1} ({2}%) listing urls.'.
                format(len(listingUrls), count, pctObtained, subClusterID))
        complete = True
        IODict[j] = self.formatSubClusterDict(
            complete, url, clickable, count, listingUrls)
        scDriver.quit()
        return

    def getSubClusters(self, driver, mainClusterDict, mainClusterNo,
                       zipcode, timeout=120):
        mode = self.subClusterMode
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

        if mode == 'parallel':
            manager = Manager()
            parallelDict = manager.dict()
            jobs = []
            timeouts = []
        else:
            seriesDict = dict()
        for j in range(len(subClusters)):
            if ('subClusters' in clusterDict.keys()) and \
               (j in clusterDict['subClusters'].keys()) and \
               (clusterDict['subClusters'][j]['complete']):
                continue
            else:
                if mode == 'parallel':
                    proc = Process(
                        target=self.scrapeSubClusterUrls,
                        args=(parallelDict, mainClusterUrl, i,
                              numSubClusters, j, count))
                    proc.start()
                    jobs.append(proc)
                else:
                    self.scrapeSubClusterUrls(
                        seriesDict, mainClusterUrl, i,
                        numSubClusters, j, count)
        if mode == 'parallel':
            for j, job in enumerate(jobs):
                job.join(timeout)
                if job.is_alive():
                    job.terminate()
                    timeouts.append(j)
                    logging.info(
                        'Subcluster {0}.{1} timed out. Had to terminate.'.
                        format(i + 1, j + 1))
            clusterDict['subClusters'] = dict(parallelDict)
            for j in timeouts:
                clusterDict['subClusters'][j] = self.formatSubClusterDict(
                    False, None, False, None, [])
        else:
            clusterDict['subClusters'] = seriesDict
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

    def getMainClusters(self, driver, mainClusterDict, zipcode):
        zc = zipcode
        origUrl = driver.current_url
        clusters = self.getClusters(driver)
        numClusters = len(clusters)
        mainClusterDict.update({'numClusters': numClusters})
        count = self.getListingCount(driver)

        logging.info(
            'Found {0} clusters in zipcode {1}.'.format(numClusters, zc))
        for i in range(numClusters):
            clusterID = i + 1
            # logging.info('*' * 100)
            logging.info(
                'Processing cluster {0} of {1}'
                ' in zipcode {2}.'.format(
                    i + 1, numClusters, zc))
            if (i in mainClusterDict['clusters'].keys()) and \
               (mainClusterDict['clusters'][i]['complete']):
                logging.info(
                    'Cluster {0} of {1} already processed with {2}% '
                    'of unique listings obtained.'.format(
                        i + 1, numClusters,
                        mainClusterDict['clusters'][i]['pctObtained']))
                # logging.info('*' * 100)
                continue
            else:
                if i not in mainClusterDict['clusters'].keys():
                    mainClusterDict['clusters'][i] = \
                        self.instantiateClusterDict()
            # logging.info('*' * 100)
            if len(clusters) != numClusters:
                logging.info(
                    'Got {0} clusters when {1} were expected. Checking'
                    'for extra tabs and closing any.'.format(
                        len(clusters), numClusters))
                self.closeExtraTabs(driver)
                clusters = self.getClusters(driver)
                if len(clusters) != numClusters:
                    logging.info(
                        'Still have got the wrong number of clusters.'
                        'Refreshing the browser.')
                    driver.refresh()
                clusters = self.getClusters(driver)
                if len(clusters) != numClusters:
                    logging.info('*' * 100)
                    logging.info(
                        'Main cluster {0} from zipcode {1}'
                        ' could not be clicked.'.
                        format(i + 1, zc).center(90, ' ').center(100, '*'))
                    logging.info('*' * 100)
                    continue
            clickable = self.clickIfClickable(
                driver, origUrl, clusters[i], clusterID)
            mainClusterDict['clusters'][i].update({'clickable': clickable})
            if clickable is False:
                logging.info('*' * 100)
                logging.info(
                    'Main cluster {0} from zipcode {1}'
                    ' could not be clicked.'.
                    format(i + 1, zc).center(90, ' ').center(100, '*'))
                logging.info('*' * 100)
                continue
            self.waitForListingsToLoad(driver, count)
            count = self.getListingCount(driver)
            mainClusterDict['clusters'][i].\
                update({'count': count, 'url': driver.current_url})
            if count > 345:
                self.checkForClusters(driver)
                self.getSubClusters(
                    driver, mainClusterDict, i, zipcode)
            else:
                listingUrls = self.getAllUrls(driver)
                pctObtained = round(len(listingUrls) / count, 3) * 100.0
                mainClusterDict['clusters'][i].update(
                    {'pctObtained': pctObtained,
                        'listingUrls': listingUrls})
            clusterInfo = mainClusterDict['clusters'][i]
            logging.info('*' * 100)
            logging.info(
                '{0} of {1} unique listings ({2}%) '
                'in cluster {3} from zipcode {4} were scraped.'.format(
                    len(clusterInfo['listingUrls']), count,
                    clusterInfo['pctObtained'], i + 1, zc).center(
                    90, ' ').center(100, '*'))
            if clusterInfo['numSubClustersOver350'] > 0:
                logging.info(
                    '{0} of {1} subclusters in cluster {2} '
                    'from zipcode {3} had more than 350 listings.'.format(
                        clusterInfo['numSubClustersOver350'],
                        clusterInfo['numSubClusters'], i + 1, zc).center(
                        90, ' ').center(100, '*'))
            if clusterInfo['numSubClustersNotClicked'] > 0:
                logging.info(
                    '{0} of {1} subclusters in cluster {2} '
                    'from zipcode {3} were not clicked.'.format(
                        clusterInfo['numSubClustersNotClicked'],
                        clusterInfo['numSubClusters'], i + 1, zc).center(
                        90, ' ').center(100, '*'))
            logging.info('*' * 100)
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
        except Exception as e:
            logging.info(e)
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

    def getUrlsByZipCode(self, zipcode):
        zc = zipcode
        allZipCodeUrls = []
        logging.info("Getting driver for zipcode {0}.".format(zc))
        driver, msg = self.goToRedfin(zc)
        self.mainDriver = driver
        if not driver:
            logging.info(msg)
            return False, msg
        logging.info("Got driver for zipcode {0}.".format(zc))
        totalListings = self.getListingCount(driver)
        logging.info(
            'Found {0} listings in zipcode {1}.'.format(totalListings, zc))
        sttm = time.time()
        mainClusterDict = self.instantiateMainClusterDict(zc)
        if totalListings < 345:
            uniqueUrls = self.getAllUrls(driver)
            numMainClusters = 0
            numClustersNotClicked = 0
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
        mainClusterDict['listingUrls'] = list(uniqueUrls)
        totalTime = round((time.time() - sttm) / 60.0, 1)
        pctObtained = round(len(uniqueUrls) / totalListings, 3) * 100.0
        logging.info('#' * 100)
        logging.info('#' * 100)
        logging.info(
            '{0} of {1} of unique listings'
            ' ({2}%) from zipcode {3} were scraped.'.format(
                len(uniqueUrls), totalListings, pctObtained,
                zc).center(90, ' ').center(100, '#').upper())
        logging.info(
            'Took {0} min. to process zipcode {1}.'.format(
                totalTime, zc).center(90, ' ').center(100, '#').upper())
        if numClustersNotClicked > 0:
            logging.info(
                '{0} of {1} main clusters from zipcode '
                '{2} were not clicked.'.format(
                    numClustersNotClicked, numMainClusters,
                    zc).center(90, ' ').center(100, '#').upper())
        logging.info('#' * 100)
        logging.info('#' * 100)
        driver.quit()
        self.pctUrlsScraped = pctObtained
        return mainClusterDict, 'ok'

    def getEventDate(self, htmlEventElement):
        rawDateStr = htmlEventElement.find_element(
            By.XPATH, './/td[contains(@class,"date-col")]').text
        dateStr = dt.strptime(rawDateStr, '%b %d, %Y').strftime('%Y-%m-%d')
        dateObj = dt.strptime(dateStr, '%Y-%m-%d')
        return dateStr, dateObj

    def getEventPrice(self, htmlEventElement):
        price = htmlEventElement.find_element(
            By.XPATH, './/td[contains(@class,"price-col")]') \
            .text.strip('$').replace(',', '')
        return price.encode('utf-8')

    def getEventSource(self, htmlEventElement):
        source = htmlEventElement.find_element(
            By.XPATH, './/div[@class="source-info"]/span[@class="source"]')
        return source.text.encode('utf-8')

    def getEventsFromListingUrl(self, url, iter_num, total_num, sttm,
                                eventQueue, urlList, timeoutList, eventsSaved):
        numEvents = 0
        eventDriver, msg = self.getChromeDriver()
        if not eventDriver:
            logging.info('Chrome could not connect to listing page {0}'.format(
                url))
            return
        eventDriver.set_page_load_timeout(160)
        try:
            eventDriver.get(url)
        except TimeoutException:
            logging.info(
                'Chrome timed out trying to get the listing at {0}'.format(
                    url))
            timeoutList.append(url)
            eventDriver.quit()
            return
        try:
            info = eventDriver.find_element(
                By.XPATH, '//div[contains(@class, "main-stats inline-block")]')
        except NoSuchElementException:
            logging.info('No property stats detected at {0}'.format(
                url))
            eventDriver.quit()
            return
        try:
            streetAddr = info.find_element(
                By.XPATH, './/span[@itemprop="streetAddress"]').text
        except StaleElementReferenceException:
            logging.info('Stale element error at {0}'.format(url))
            return
        try:
            cityStateZip = info.find_element(
                By.XPATH, './/span[@class="citystatezip"]')
            city = cityStateZip.find_element(
                By.XPATH, './/span[@class="locality"]').text.strip(',')
            state = cityStateZip.find_element(
                By.XPATH, './/span[@class="region"]').text
            zipcode = cityStateZip.find_element(
                By.XPATH, './/span[@class="postal-code"]').text
            lat = info.find_element(
                By.XPATH,
                './/span[@itemprop="geo"]/meta[@itemprop="latitude"]') \
                .get_attribute('content')
            lon = info.find_element(
                By.XPATH,
                './/span[@itemprop="geo"]/meta[@itemprop="longitude"]') \
                .get_attribute('content')
        except NoSuchElementException:
            logging.info('No locational stats detected at {0}'.format(url))
            return
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

        factsTable = eventDriver.find_element(
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
        try:
            apn = factDict['APN']
        except KeyError:
            apn = None
        staticAttrs = [apn, streetAddr, city, state, zipcode,
                       lat, lon, beds, baths, sqft, lotSize, propType,
                       yearBuilt, eventDriver.current_url]
        historyRows = eventDriver.find_elements(
            By.XPATH, '//*[contains(@id,"propertyHistory")]')
        if historyRows is None:
            logging.info('No property history at {0}'.format(
                url))
            eventDriver.quit()
            return
        lastEvent = None
        saleDtStr = None
        saleDtObj = None
        salePrice = None
        saleMLS = None
        for i, historyRow in enumerate(historyRows):
            if 'Sold' in historyRow.text:
                curSaleDtStr, curSaleDtObj = self.getEventDate(historyRow)
                curSalePrice = self.getEventPrice(historyRow)
                curSaleMLS = self.getEventSource(historyRow)
                if (lastEvent == 'sale') and (
                    (saleDtObj is None) or
                    ((saleDtObj is not None) and
                     (curSaleDtObj < saleDtObj - td(days=14)))):
                    eventQueue.put(
                        staticAttrs +
                        [saleDtStr, salePrice, None, None, saleMLS])
                    numEvents += 1
                if i + 1 == len(historyRows):
                    eventQueue.put(
                        staticAttrs +
                        [curSaleDtStr, curSalePrice, None, None, curSaleMLS])
                    numEvents += 1
                lastEvent = 'sale'
                saleDtStr, saleDtObj, salePrice, saleMLS = \
                    curSaleDtStr, curSaleDtObj, curSalePrice, curSaleMLS
            elif 'Listed' in historyRow.text and lastEvent == 'sale':
                listDtStr, listDtObj = self.getEventDate(historyRow)
                listPrice = self.getEventPrice(historyRow)
                eventQueue.put(
                    staticAttrs +
                    [saleDtStr, salePrice, listDtStr, listPrice, saleMLS])
                numEvents += 1
                lastEvent = 'listing'
            else:
                continue
        eventDriver.quit()
        durMins, minsLeft = self.timeElapsedLeft(sttm, iter_num + 1, total_num)
        if (i + 1) % 10 == 0:
            logging.info(
                'Scraped {0} sales events from listing {1} of {2}.'
                ' Saved {3} total sales events in {4} min.'
                ' Estimated time to completion: ~{5} min.'.format(
                    numEvents, iter_num + 1, total_num, eventsSaved,
                    durMins, minsLeft))
        urlList.append(url)
        return

    def pickleClusterDict(self, clusterDict, zipcode):
        pickle.dump(clusterDict, open(
            'data/pickles/main_cluster_dict_{0}.pkl'.format(zipcode), 'w+'))

    def timeElapsedLeft(self, sttm, iterNum, totalNum):
        duration = time.time() - sttm
        durMins = round(duration / 60.0, 1)
        minsPerListing = durMins / (iterNum + 1)
        minsLeft = round(minsPerListing * (totalNum - iterNum - 1), 1)
        return durMins, minsLeft

    def eventWorker(self, queue):
        while not queue.empty():
            task = queue.get()
            url, i, numUrls, sttm, eventQueue, \
                urlList, timeoutList, eventsSaved = task
            self.getEventsFromListingUrl(
                url, i, numUrls, sttm, eventQueue, urlList, timeoutList,
                eventsSaved.value)

    def writeToCsvWorker(self, queue, eventsSaved):
        with open(self.eventFile, 'a+') as f:
            writer = csv.writer(f)
            while True:
                try:
                    event = queue.get(block=True)
                    writer.writerow(event)
                    eventsSaved.value += 1
                except Empty:
                    break

    def writeEventsToCsv(self, urls, processedUrlsFName):
        numUrls = len(urls)
        urlsWithEvents = 0
        totalEvents = 0

        try:
            with open(processedUrlsFName, 'r') as pus:
                pUrls = list(set(pus.read().split('\r\n')))
            logging.info(
                'Already processed {0} of {1} urls. Picking up where we'
                ' left off.'.format(len(pUrls), numUrls))
            urls = [url for url in urls if url not in pUrls]
            numUrls = len(urls)
        except IOError:
            pass

        with open(processedUrlsFName, 'a+') as pus:
            pUrls_writer = csv.writer(pus)
            with open(self.eventFile, 'a+') as f:
                writer = csv.writer(f)
                sttm = time.time()

                if self.eventMode == 'parallel':
                    manager = Manager()
                    timeoutList = manager.list()
                    urlList = manager.list(urls)
                    queue = Queue()
                    eventQueue = manager.Queue()
                    eventsSaved = manager.Value('i', 0)
                    jobs = []
                    for i, url in enumerate(urls):
                        queue.put(
                            [url, i, numUrls, sttm, eventQueue, urlList,
                             timeoutList, eventsSaved])
                    for i in range(min(24, numUrls)):
                        proc = Process(target=self.eventWorker, args=(queue,))
                        proc.start()
                        jobs.append(proc)
                    writeProc = Process(target=self.writeToCsvWorker, args=(
                        eventQueue, eventsSaved))
                    time.sleep(2)
                    writeProc.start()
                    for j, job in enumerate(jobs):
                        # 5 seconds per url for each process before timeout
                        job.join(max(60, 5 * numUrls))
                        if job.is_alive():
                            job.terminate()
                            logging.info(
                                'Subprocess {0} of {1} timed out'.format(
                                    j + 1, min(24, numUrls)))
                    writeProc.join(max(60, 8 * numUrls))
                    totalEvents = eventsSaved.value
                    for url in set(list(urlList)):
                        pUrls_writer.writerow([url])
                    urlsWithEvents = len(set(list(urlList)))
                    numTimeouts = len(set(list(timeoutList)))

                else:
                    for i, url in enumerate(urls):
                        numEvents = 0
                        events = self.getEventsFromListingUrl(url)
                        if events is None:
                            durMins, minsLeft = self.timeElapsedLeft(
                                sttm, i, numUrls)
                            logging.info(
                                'No sales events scraped from listing'
                                ' {0} of {1}. Check url: {2}. {3} min.'
                                'elapsed. {4} min. remaining.'.format(
                                    i + 1, numUrls, url, durMins,
                                    minsLeft))
                            continue
                        for event in events:
                            totalEvents += 1
                            numEvents += 1
                            writer.writerow(event)
                        urlsWithEvents += 1
                        pUrls_writer.writerow([url])
                        durMins, minsLeft = self.timeElapsedLeft(
                            sttm, i, numUrls)
                        if (i + 1) % 10 == 0:
                            logging.info(
                                'Scraped {0} sales events from listing {1}'
                                ' of {2}. Scraped {3} total sales events in'
                                ' {4} min. Estimated time to completion:'
                                ' ~{5} min.'.format(
                                    numEvents, i + 1, numUrls, totalEvents,
                                    durMins, minsLeft))
        if numUrls > 0:
            self.pctUrlsWithEvents = round(
                urlsWithEvents / numUrls, 1) * 100
        else:
            self.pctUrlsWithEvents = -999

        logging.info('#' * 100)
        logging.info('#' * 100)
        logging.info(
            'Scraped events from {0} of {1} ({2}%) urls.'.format(
                urlsWithEvents, numUrls, self.pctUrlsWithEvents).center(
                90, ' ').center(100, '#').upper())
        logging.info(
            ('{0} of {1} urls timed out while scraping events.'.format(
                numTimeouts, numUrls).upper().center(90, ' ').center(
                100, '#')))
        logging.info(
            ('Saved {0} events to {1}'.format(
                totalEvents, self.eventFile).upper().center(
                90, ' ').center(100, '#')))
        logging.info('#' * 100)
        logging.info('#' * 100)

    def writeCsvToDb(self):
        dbname = 'redfin'
        host = 'localhost'
        port = 5432
        conn_str = "dbname={0} host={1} port={2}".format(dbname, host, port)
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        with open(self.eventFile, "r") as f:
            events = csv.reader(f, delimiter=',')
            eventList = list(events)
        numEvents = len(eventList)
        numWritten = 0
        dupes = 0
        logging.info('Writing {0} events to redfin database.'.format(
            numEvents))
        for row in eventList:
            row = [x if x not in ['', '*', '**', '-', '\xe2\x80\x94']
                   else None for x in row]
            if len(row) != 19:
                continue
            insertStr = 'INSERT INTO sales_listings VALUES (' + \
                ','.join(['%s'] * 19) + ')'
            try:
                cur.execute(insertStr, row)
                conn.commit()
                numWritten += 1
            except IntegrityError as e:
                if str(e.pgcode) == '23505':
                    dupes += 1
                else:
                    logging.info(str(e.pgerror))
                conn.rollback()
                continue
            except InterfaceError, e:
                logging.info(str(e))
                conn = psycopg2.connect(conn_str)
                cur = conn.cursor()
                continue
        if numEvents > 0:
            self.pctEventsWritten = round(numWritten / numEvents, 2) * 100
        else:
            self.pctEventsWritten = -999
        logging.info('#' * 100)
        logging.info('#' * 100)
        logging.info(
            'Wrote {0} of {1} ({2}%) events to redfin database.'.
            format(numWritten, numEvents, self.pctEventsWritten).upper(
            ).center(90, ' ').center(100, '#'))
        logging.info(
            '{0} of {1} events were duplicates.'.
            format(dupes, numEvents).upper(
            ).center(90, ' ').center(100, '#'))
        logging.info('#' * 100)
        logging.info('#' * 100)
        cur.close()
        conn.close()
        return

    def run(self, zipcode):
        sttm = time.time()
        zc = zipcode
        mainClusterDict, msg = self.getUrlsByZipCode(zc)
        if not mainClusterDict:
            return
        else:
            self.mainClusterDict = mainClusterDict
            self.pickleClusterDict(mainClusterDict, zc)
        allZipCodeUrls = mainClusterDict['listingUrls']
        self.listingUrls = allZipCodeUrls
        self.writeEventsToCsv(allZipCodeUrls, self.processedUrlsFName)
        self.writeCsvToDb()
        dur = round(time.time() - sttm / 60.0, 1)
        logging.info('%' * 100)
        logging.info('%' * 100)
        logging.info('Took {0} minutes to process zipcode {1}'.format(
            dur, zc).upper().center(90, ' ').center(100, '%'))
        logging.info('%' * 100)
        logging.info('%' * 100)
        return
