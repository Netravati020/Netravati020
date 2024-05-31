# -*- coding: utf-8 -*-
"""
Created on Mon Oct  2 10:36:34 2023

@author: DURGA_PRASAD_REJETI

"""

import time
import datetime
from selenium.webdriver.common.by import By
from selenium import webdriver
import pandas as pd
import time
import re
from bs4 import BeautifulSoup
from time import strftime
current_month = strftime('%B')
webdriver= webdriver.Chrome(r'C:\temp\chromedriver\chromedriver.exe')

# webdriver = webdriver.Chrome(r'C:\Users\DURGA_PRASAD_REJETI\Downloads\chromedriver.exe')

url = "https://www.google.com"
webdriver.get(url)
webdriver.get('https://www.channel4.com')
time.sleep(3)
webdriver.find_element_by_xpath("//button[@aria-label='Accept cookies and continue.']").click()

df=pd.read_excel(r'all4_tv_links_'+current_month+'_2024'+'.xlsx')
# df=df[:4]
def load_page():
    for i in range(3):
        try:
            webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
        except:
            pass
def extractor():
    try:
        next_page = True
        while next_page:
            load_page()
            # webdriver.execute_script("window.scrollTo(0, window.scrollY + 2000)")
            if 'all4-secondary-button all4-typography-body all4-episode-list__button' in str(webdriver.page_source):
                webdriver.find_element_by_xpath(
                    "//button[@class='all4-secondary-button all4-typography-body all4-episode-list__button ']").click()
                print('has more page')
                time.sleep(2)
            else:
                print('no more page')
                next_page = False
    except:
        pass


df_tvshows = pd.DataFrame(columns=['Content Type', 'Service', 'Country', 'Collection Date', 'Title',
                                   'Year', 'Month', 'Day', 'Season Number', 'Episode Number', 'Episode Name',
                                   'Number Episodes', 'Rating', 'Currency', 'Price SD Rent', 'Price SD Buy',
                                   'Price HD Rent', 'Price HD Buy', 'Genres', 'Duration (minutes)', 'Network',
                                   'Synopsis', 'Language', 'Production Company', 'Studio', 'Cast', 'Director',
                                   'Writer', 'Format', 'Season URL', 'Episode URL', 'Episode Synopsis'])

for a in df.iterrows():
    try:
        print("hit no of url",a[1][1])
        webdriver.get(a[1][1])
        current_url = a[1][1]
        time.sleep(3)
        soup = BeautifulSoup(webdriver.page_source, 'html.parser')
        extractor()

        content = "Tv Show"
        service = "ALL4"
        country = "UK"
        month = ""
        day = ""
        year = ""
        currency = ""
        sdrent = ""
        sdbuy = ""
        hdrent = ""
        hdbuy = ""
        cast = ""
        director = ""
        genre = ""
        writer = ""
        synopsis = ""
        duration = ""
        rating = ""
        season_no = ""
        show_url = ""
        Episode_no = ""
        Episode_name = ""
        Episode_Synopsis = ""
        no_epi = ""
        episode_url = ""

        for d in webdriver.find_elements("xpath", "//div[@class='all4-episode-list-item__content']"):
            Episode_name = d.find_element(By.CSS_SELECTOR, "h3").text
            Episode_Synopsis = d.find_element(By.CSS_SELECTOR, "p").text
            ep_dur = d.find_element(By.CSS_SELECTOR, "div").text
            try:
                year1= d.find_element('xpath','.//div[@class="all4-caption-text all4-typography-caption all4-episode-list-item__bottom-area-text  secondary aligned-left"]').text.split("|")[0].strip().replace('First shown:','').strip()
                print(year1)
                if ',' in year1:
                    try:
                        year_digit = re.findall('(\d{4})', year1)[0]
                    except:
                        year_digit=''
                        print('year not found')
                    year1 = year1.split(',')[0]
                    date_object = datetime.datetime.strptime(year1, "%a %d %b")
                    # formatted_date = date_object.strftime("%d %m %Y")
                    # split_year1 = year1.split(' ')
                    month_digit = date_object.month
                    day_digit = date_object.day
                    year_digit = ''
                else:
                    date_object = datetime.datetime.strptime(year1, "%a %d %b %Y")
                    # formatted_date = date_object.strftime("%d %m %Y")
                    # split_year1= year1.split(' ')
                    month_digit = date_object.month
                    day_digit = date_object.day
                    year_digit = date_object.year


                # date_object = datetime.strptime(year1, "%a %d %b %Y")
                # formatted_date = date_object.strftime("%d %m %Y")
                # # print("Formatted Date:", formatted_date)
                # month_digit = formatted_date.month
                # day_digit = formatted_date.day
                # year_digit = formatted_date.year

                # Print the results
                print("Month:", month_digit)
                print("Day:", day_digit)
                print("Year:", year_digit)
            except:
                month_digit = ''
                day_digit= ''
                year_digit= ''
                year1=""
            try:
                dur = ep_dur.split('|')[0]
                Dur = ep_dur.split('|')[1]
            except:
                pass
            if 'mins' in dur:
                print(dur.replace(' mins', ''))
                dura = dur.replace(' mins', '')
            else:
                print(Dur.replace(' mins', ''))
                dura = Dur.replace(' mins', '')
            duration = dura

            try:
                title = soup.find('title').text.split('|')[0].replace('Watch ', '')
                print("title:",title)
            except:
                title = ''

            try:
                synopsis = webdriver.find_element("xpath","//div[@class='all4-brandhubs-details__description']").text
                print(synopsis)
            except:
                synopsis = ''

            try:
                genre = webdriver.find_element("xpath","//div[@class='all4-caption-text all4-typography-caption all4-brandhubs-details-text mobile-two-lines  secondary']").text
                print(genre)
            except:
                genre = ''

            try:
                show_url = current_url
            except:
                show_url = ''

            try:
                season_no = webdriver.find_element("xpath",
                                                   "//span[@class='tertiary-icon-button__label all4-typography-body']").text

            except:
                season_no = ''

            final_data = {'Content Type': 'Tv Show', 'Service': 'ALL4', 'Country': 'UK',
                          'Collection Date': datetime.date.today().strftime('%m/%d/%Y'), 'Title': title,
                          'Year': year_digit, 'Month': month_digit, 'Day': day_digit, 'Season Number': season_no,
                          'Episode Number': Episode_no, 'Episode Name': Episode_name,
                          'Number Episodes': '', 'Rating': rating, 'Currency': '', 'Price SD Rent': '',
                          'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                          'Genres': genre, 'Duration (minutes)': duration.strip(),
                          'Network': '', 'Synopsis': synopsis, 'Language': '',
                          'Production Company': '', 'Studio': '', 'Cast': cast, 'Director': director, 'Writer': '',
                          'Format': '', 'Season URL': show_url, 'Episode URL': episode_url,
                          'Episode Synopsis': Episode_Synopsis}
            print(final_data)
            df_tvshows = pd.concat([df_tvshows, pd.DataFrame([final_data])], ignore_index=True)

        seasons = webdriver.find_elements("xpath", "//li[@class='all4-typography-body all4-list__item']")
        for s in range(0, len(seasons)):
            webdriver.find_element("xpath", "//div[@class='all4-menu']").click()
            time.sleep(2)
            webdriver.find_elements("xpath", "//li[@class='all4-typography-body all4-list__item']")[s].click()

            for i in range(0, 5):
                extractor()

            for d in webdriver.find_elements("xpath", "//div[@class='all4-episode-list-item__content']"):
                Episode_name = d.find_element(By.CSS_SELECTOR, "h3").text
                Episode_Synopsis = d.find_element(By.CSS_SELECTOR, "p").text
                ep_dur = d.find_element(By.CSS_SELECTOR, "div").text

                try:
                    year1 = d.find_element('xpath',
                                           './/div[@class="all4-caption-text all4-typography-caption all4-episode-list-item__bottom-area-text  secondary aligned-left"]').text.split(
                        "|")[0].strip().replace('First shown:', '').strip()
                    print(year1)
                    if ',' in year1:
                        try:
                            year_digit = re.findall('(\d{4})', year1)[0]
                        except:
                            year_digit = ''
                            print('year not found')
                        year1 = year1.split(',')[0]
                        date_object = datetime.datetime.strptime(year1, "%a %d %b")
                        # formatted_date = date_object.strftime("%d %m %Y")
                        # split_year1 = year1.split(' ')
                        month_digit = date_object.month
                        day_digit = date_object.day
                        year_digit = ''
                    else:
                        date_object = datetime.datetime.strptime(year1, "%a %d %b %Y")
                        # formatted_date = date_object.strftime("%d %m %Y")
                        # split_year1= year1.split(' ')
                        month_digit = date_object.month
                        day_digit = date_object.day
                        year_digit = date_object.year

                    # date_object = datetime.strptime(year1, "%a %d %b %Y")
                    # formatted_date = date_object.strftime("%d %m %Y")
                    # # print("Formatted Date:", formatted_date)
                    # month_digit = formatted_date.month
                    # day_digit = formatted_date.day
                    # year_digit = formatted_date.year

                    # Print the results
                    print("Month:", month_digit)
                    print("Day:", day_digit)
                    print("Year:", year_digit)
                except:
                    month_digit = ''
                    day_digit = ''
                    year_digit = ''
                    year1 = ""

                try:
                    dur = ep_dur.split('|')[0]
                    Dur = ep_dur.split('|')[1]
                except:
                    pass
                if 'mins' in dur:
                    print(dur.replace(' mins', ''))
                    dura = dur.replace(' mins', '')
                else:
                    print(Dur.replace(' mins', ''))
                    dura = Dur.replace(' mins', '')
                duration = dura

                try:
                    title = soup.find('title').text.split('|')[0].replace('Watch ', '')

                except:
                    title = ''

                try:
                    show_url = current_url
                except:
                    show_url = ''

                try:
                    season_no = webdriver.find_element("xpath","//span[@class='tertiary-icon-button__label all4-typography-body']").text

                except:
                    season_no = ''

                final_data = {'Content Type': 'Tv Show', 'Service': 'ALL4', 'Country': 'UK',
                              'Collection Date': datetime.date.today().strftime('%m/%d/%Y'), 'Title': title,
                              'Year': year_digit, 'Month': month_digit, 'Day': day_digit, 'Season Number': season_no,
                              'Episode Number': Episode_no, 'Episode Name': Episode_name,
                              'Number Episodes': '', 'Rating': rating, 'Currency': '', 'Price SD Rent': '',
                              'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                              'Genres': genre, 'Duration (minutes)': duration.strip(),
                              'Network': '', 'Synopsis': synopsis, 'Language': '',
                              'Production Company': '', 'Studio': '', 'Cast': cast, 'Director': director, 'Writer': '',
                              'Format': '', 'Season URL': show_url, 'Episode URL': episode_url,
                              'Episode Synopsis': Episode_Synopsis}
                print(final_data)
                df_tvshows = pd.concat([df_tvshows, pd.DataFrame([final_data])], ignore_index=True)

    except Exception as e:
        print(e)
        df_tvshows.to_excel(r'All_4_Tv_Shows_dp_' + current_month + '.xlsx')
df_tvshows.to_excel(r'All_4_Tv_Shows_dp_'+current_month+'.xlsx')
webdriver.close()