from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
import time
from time import strftime

current_month = strftime('%B')
webdriver= webdriver.Chrome(r'C:\temp\chromedriver\chromedriver.exe')

# webdriver = webdriver.Chrome(r'C:\Users\BOT_MEDIA_VOD_3\Downloads\chromedriver.exe')
url = "https://www.channel4.com/categories"
# url='https://www.channel4.com/categories?sort=az'
webdriver.get(url)
time.sleep(5)
url = []

def load_page():
    for i in range(3):
        try:
            # webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
            webdriver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
            time.sleep(2)
        except:
            pass

try:
    next_page = True
    while next_page:
        load_page()
        # webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
        if 'all4-secondary-button all4-typography-body' in str(webdriver.page_source):
            show_more =webdriver.find_element_by_xpath("//button[@class='all4-secondary-button all4-typography-body']").click()
            print('has more page')
            time.sleep(2)
        else:
            print('no more page')
            next_page=False
except:
    pass

for j in webdriver.find_elements_by_xpath("//a[@class='all4-slice-item']"):
    print(j.get_attribute('href'))
    url.append(j.get_attribute('href'))

df = pd.DataFrame()
df["url"] = url
print(len(df["url"]))

df.to_excel(r'all4_tv_links_'+current_month+'_2024'+'.xlsx')
webdriver.close()