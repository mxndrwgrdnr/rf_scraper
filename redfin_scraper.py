import time
import csv
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pyzipcode import ZipCodeDatabase

display = Display(visible=0, size=(800, 600))
display.start()

chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory": "/home/mgardner/rf_scraper/data/"}
chromeOptions.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(chrome_options=chromeOptions)

zcdb = ZipCodeDatabase()
zips = [zc.zip for zc in zcdb.find_zip()]
failures = []
# zips = ['94609', '94110', '00501', '00210']

for zc in zips:

    url = "https://www.redfin.com/zipcode/" + zc
    print "trying " + zc
    driver.get(url)

    if driver.current_url == 'https://www.redfin.com/out-of-area-signup':
        print('no listings for ' + url)
        failures.append(zc)

    else:
        try:
            WebDriverWait(driver, 3).until(
                EC.presence_of_element_located(
                    (By.CLASS_NAME, "downloadLink"))).click()
            print "got " + zc
        except:
            print "timeout getting " + zc
            failures.append(zc)

    time.sleep(0.1)

with open('failures.csv', 'wb') as f:
    wr = csv.writer(f)
    wr.writerows(failures)
