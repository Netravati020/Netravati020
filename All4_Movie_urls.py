from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
import time
from time import strftime

current_month = strftime('%B')
webdriver= webdriver.Chrome(r'C:\temp\chromedriver\chromedriver.exe')

# webdriver = webdriver.Chrome(r'C:\Users\BOT_MEDIA_VOD_3\Downloads\chromedriver.exe')
url = "https://www.channel4.com/categories/film"
# url='https://www.channel4.com/categories/film?sort=az'
webdriver.get(url)

url = []
for i in range(0, 10):
    for i in range(0, 10):
        try:
            webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
        except:
            pass
    try:
        webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
        webdriver.find_element_by_xpath("//button[@class='all4-secondary-button all4-typography-body']").click()
        time.sleep(3)
    except:
        pass
for i in webdriver.find_elements_by_xpath("//a[@class='all4-slice-item']"):
    print(i.get_attribute('href'))
    url.append(i.get_attribute('href'))

df = pd.DataFrame()
df["Url"] = url

df.to_excel(r'all4_movie_links_'+current_month+'_2024'+'.xlsx')
webdriver.close()

