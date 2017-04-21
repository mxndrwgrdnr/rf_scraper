import csv
import redfin_scraper
from pyzipcode import ZipCodeDatabase
from datetime import datetime as dt

rf = redfin_scraper.redfinScraper(
    virtualDisplay=True, subClusterMode='parallel',
    timeFilter='sold-all')

zcdb = ZipCodeDatabase()
# zips = [zc.zip for zc in zcdb.find_zip()]
zips = ['94609']
sttm = dt.now().strftime('%Y%m%d-%H%M%S')
dataDir = './data'

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]

for zc in zips:

    outfile = dataDir + '/historic_sales/events_' + zc + '_' + sttm + '.csv'
    processedUrlsFName = dataDir + '/processed_urls/processed_urls_' + \
        zc + '.csv'
    if zc in not_listed:
        continue

    mainClusterDict, msg = rf.getUrlsByZipCode(zc)
    if not mainClusterDict:
        if msg == 'out of area':
            not_listed += [zc]
    else:
        rf.pickleClusterDict(mainClusterDict, zc)

    allZipCodeUrls = mainClusterDict['listingUrls']
    rf.writeEventsToCsv(zc, outfile, allZipCodeUrls, processedUrlsFName)
    rf.writeCsvToDb(outfile)
    break
