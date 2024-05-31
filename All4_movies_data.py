import time
import datetime
import re
import requests
import json
from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
from time import strftime

current_month = strftime('%B')
# webdriver = webdriver.Chrome(r'C:\Users\DURGA_PRASAD_REJETI\Downloads\chromedriver.exe')
webdriver= webdriver.Chrome(r'C:\temp\chromedriver\chromedriver.exe')

url = "https://www.google.com"
webdriver.get(url)

df = pd.read_excel(r"C:\Users\BOT_MEDIA_VOD_2\Dhruv\uk01\ALL4\all4_movie_links_April_2024.xlsx")

df_movies = pd.DataFrame(columns=['Content Type', 'Service', 'Country', 'Collection Date', 'Title', 'Year', 'Month',
                                  'Day', 'Rating', 'Currency', 'Price SD Rent', 'Price SD Buy',
                                  'Price HD Rent', 'Price HD Buy', 'Genre', 'Duration (minutes)', 'Network', 'Synopsis',
                                  'Language', 'Production', 'Studio', 'Cast', 'Director', 'Writer', 'Format',
                                  'URL'])

for m in range(0, len(df.Url)):
    print(m)
    webdriver.get(df.Url[m])
    time.sleep(3)
    soup = BeautifulSoup(webdriver.page_source, 'html.parser')

    content = "Movie"
    service = "All4"
    country = "UK"
    z = datetime.datetime.now()
    ab = str(z.strftime('%d'))
    bb = str(z.year)
    cb = str(z.strftime('%m'))
    collectiondate = cb + "/" + ab + "/" + bb
    title = ""
    month = ""
    day = ""
    currency = ""
    sdrent = ""
    sdbuy = ""
    hdrent = ""
    hdbuy = ""
    language = ""
    studio = ""
    rating = ""
    mov_format = ""
    cast = ""
    year = ""
    duration = ""
    network = ''
    synopsis = ""
    director = ""
    production_company = ""
    writer = ""
    genre = ""

    movie_url = df.Url[m]

    print(movie_url)

    try:
        synopsis = webdriver.find_element("xpath","//div[@class='all4-brandhubs-details__description']").text
        print("synopsis", synopsis)
    except:
        synopsis = ""

    try:
        year=synopsis[0:7].replace('(', '').replace(') ', '')
        print("year", year)
    except:
        year=''

    try:
        title = soup.find('title').text.split('|')[0].replace("Watch","")
        print("Title",title)

    except:
        title = ""

    try:

        # duration_1 = webdriver.find_element("xpath","//div[@class='all4-caption-text all4-typography-caption all4-brandhubs-details-text  secondary']").text
        duration= webdriver.find_element("xpath","//div[@class='all4-caption-text all4-typography-caption all4-brandhubs-details-text mobile-two-lines  secondary']").text
        if ' | ' in duration:
            split_section = duration.split(" | ")
            genre = split_section[0]
            print("genre:", genre)
            duration=split_section[-1]
            if 'mins' in duration:
                duration =duration.replace('mins', '')
                print("duration:",duration)
            else:
                duration = split_section[-2].replace('mins', '')
                print("duration:", duration)
        else:
            genre = duration.strip()
            duration = ''
            print("genre:", genre)

        if duration.isdigit():
            pass
        else:
            duration=''
    except:
        duration = ""
        genre=''

    final_data = {'Content Type': 'Movie', 'Service': service, 'Country': country, 'Collection Date': collectiondate,
                  'Title': title,
                  'Year': year, 'Month': month, 'Day': day, 'Rating': rating, 'Currency': currency,
                  'Price SD Rent': sdrent, 'Price SD Buy': sdbuy,
                  'Price HD Rent': hdrent, 'Price HD Buy': hdbuy, 'Genre': genre, 'Duration (minutes)': duration,
                  'Network': network,
                  'Synopsis': synopsis, 'Language': language, 'Production': production_company, 'Studio': studio,
                  'Cast': cast,
                  'Director': director, 'Writer': writer, 'Format': mov_format, 'URL': movie_url}
    print(final_data)

    # df_movies = df_movies.append(final_data, ignore_index=True)
    df_movies = pd.concat([df_movies, pd.DataFrame([final_data])], ignore_index=True)

df_movies.to_excel(r'all4_movie_data_'+current_month + "_2024"+".xlsx")
webdriver.close()