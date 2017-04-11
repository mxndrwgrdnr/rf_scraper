import logging
import csv
import redfin_scraper
import time

rf = redfin_scraper.redfinScraper(virtualDisplay=True)




# zcdb = ZipCodeDatabase()
# zips = [zc.zip for zc in zcdb.find_zip()]
zips = ['44122']
# startDate = dt.now().strftime('%Y-%m-%d')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]

mainClusterDict, newNotListed = rf.getUrlsByZipCode(zips, not_listed)


    