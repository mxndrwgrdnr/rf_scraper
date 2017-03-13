from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.chrome.options import Options

display = Display(visible=0, size=(800, 600))
display.start()

chrome_options = Options()
chrome_options.add_extension("/home/mgardner/rf_scraper/proxy.zip")
chrome_options.add_argument('--ignore-certificate-errors')

driver = webdriver.Chrome(chrome_options=chrome_options)
driver.get("http://icanhazip.com")
