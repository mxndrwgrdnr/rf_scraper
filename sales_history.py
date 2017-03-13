import csv
from selenium import webdriver
from pyvirtualdisplay import Display
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from pyzipcode import ZipCodeDatabase
from selenium.webdriver.chrome.options import Options



display = Display(visible=0, size=(800, 600))
display.start()

chrome_options = Options()
chrome_options.add_extension("/home/mgardner/rf_scraper/proxy.zip")

driver = webdriver.Chrome(chrome_options=chrome_options)

def get_urls(
    dbname = 'redfin',
    host='localhost',
    port=5432,
    username='mgardner',
    passwd=None)
    conn_str = "dbname={0} user={1} host={2} password={3} port={4}".format(dbname,username,host,passwd,port)
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    num_listings = len(dataframe)

    prob_PIDs = []
    dupes = []
    writes = []
    for i,row in dataframe.iterrows():
        try:
            cur.execute('''INSERT INTO rental_listings
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
