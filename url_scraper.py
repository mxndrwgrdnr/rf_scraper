import csv
import redfin_scraper
from pyzipcode import ZipCodeDatabase
from datetime import datetime as dt
from selenium.webdriver.chrome.options import Options
import os

os.environ["DBUS_SESSION_BUS_ADDRESS"] = '/dev/null'
zcdb = ZipCodeDatabase()
zips = [zc.zip for zc in zcdb.find_zip()]
sttm = dt.now().strftime('%Y%m%d-%H%M%S')
dataDir = './data'
chrome_options = Options()
chrome_options.add_extension("./proxy.zip")
chrome_options.add_argument("--ignore-certificate-errors")
chrome_options.add_argument("--window-size=1024,768")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-infobars")
sttm = dt.now().strftime('%Y%m%d-%H%M%S')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]

with open('./processed_zips.csv', 'rb') as f:
    reader = csv.reader(f)
    processed = [row[0] for row in reader]

for zc in zips:

    if zc in not_listed or zc in processed:
        continue

    eventFile = '/historic_sales/events_' + zc + '.csv'
    processedUrlsFName = '/processed_urls/processed_urls_' + zc + '.csv'

    rf = redfin_scraper.redfinScraper(
        eventFile, processedUrlsFName, virtualDisplay=True,
        subClusterMode='parallel', eventMode='parallel', timeFilter='sold-all',
        dataDir=dataDir, startTime=sttm, chromeOptions=chrome_options)

    driver = rf.run(zc)
    with open('./processed_zips.csv', 'a+') as f:
        zipWriter = csv.writer(f)
        zipWriter.writerow([
            zc, rf.pctUrlsScraped, rf.pctUrlsWithEvents,
            rf.pctEventsWritten])
