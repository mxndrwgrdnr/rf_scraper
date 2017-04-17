import csv
import redfin_scraper

rf = redfin_scraper.redfinScraper(virtualDisplay=False)

# zcdb = ZipCodeDatabase()
# zips = [zc.zip for zc in zcdb.find_zip()]
zips = ['94609']
# startDate = dt.now().strftime('%Y-%m-%d')

with open('not_listed.csv', 'rb') as f:
    reader = csv.reader(f)
    not_listed = [zc for zclist in reader for zc in zclist]

mainClusterDict, newNotListed = rf.getUrlsByZipCode(zips, not_listed)
