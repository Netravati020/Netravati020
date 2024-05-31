# -*- coding: utf-8 -*-
"""
objective = extract Movies and Tv Show urls and extracting required datapoints from Movies and Tv Show

Created on April 6  2024

@author: Netravati

"""
import sys, pandas as pd
from datetime import time

from MediaVOD.library_processor.library_process_base import BaseCrawling
from WISE.wise_crawling_wrapper import Crawling_Wrap_selenium
import json
from bs4 import BeautifulSoup
class process(BaseCrawling):
    def __init__(self, utils):
        super().__init__(utils)

        # use selenium class from wrapper and initialize_chrome_driver
        self.crawl_wrapper = Crawling_Wrap_selenium(self.utils)
        self.crawl_wrapper.initialize_chrome_driver()

        # put sleep that you observed in website
        self.random_sleep = self.crawl_wrapper.get_random_number(9, 12)

        # call generic process for calling all functions for crawling
        self.library_genric_process_1(self.movie_url_extracting, self.movie_data_extracting,
                                      self.Tv_show_url_extracting, self.Tv_show_data_extracting)

        # close driver
        self.crawl_wrapper.close()

    def movie_url_extracting(self):
        """
           Collects movie URLs from a specified base URL and genre URLs.

           This method performs the following steps:
           1. Checks if movie URL extraction has already been completed by checking a flag.
           2. Gathers genre URLs from the base movie URL.
           3. Iterates through genre URLs to collect movie URLs.
           4. Stores the collected movie URLs in a DataFrame, removes duplicates, and saves to an Excel file.
           5. Sends an email alert upon successful completion.

           Raises:
               Exception: If any error occurs during the movie URL collection process.

           Note:
               This method relies on certain configurations specified in the 'url_path' section of the config.
        """
        # region xpath of movie url collection

        base_movie_url_xpath = self.utils.xpaths_dict['movie_url']
        movie_genre_url_xpath = self.utils.xpaths_dict['genre_xpath']
        movie_urls_collection_xpath = self.utils.xpaths_dict['movie_url_xpath']
        self.movie_url_input_file_name = 'Movies_url'

        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.movie_url_input_file_name):
                self.logger.info('Movie url collection already Finished', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            Genre_urls = []
            movie_urls = []

            # logger.info function inserting logs in file

            self.logger.info("Starting Movie url collection ", Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # base_movie_url is where we are find movies urls

            self.crawl_wrapper.open_url(base_movie_url_xpath, self.random_sleep)
            # self.logger.info('Hit base movie url = ' + base_movie_url_xpath)

            # collection of genre_urls

            self.crawl_wrapper.one_time_scroll(self.random_sleep)

            # self.logger.info('Start gather genre urls ----------------------------------')

            # xpath of getting genre urls

            for Genre_url_tag in self.crawl_wrapper.find_info("xpath", movie_genre_url_xpath,
                                                              type_of_element='elements'):
                Genre_url = self.crawl_wrapper.get_href_value(Genre_url_tag)
                if Genre_url == '':
                    continue
                Genre_urls.append(Genre_url)

                # self.logger.info('Find this genre url = ' + Genre_url)

            # collection of movie urls from genre urls
            # self.logger.info('Start gather movie urls ----------------------------------')
            for Genre_url in Genre_urls:
                self.crawl_wrapper.open_url(Genre_url, self.random_sleep)
                # self.logger.info('Hit this genre url  = ' + str(Genre_url))

                # Use scrolling function of media wrap
                self.crawl_wrapper.scrolling_page_with_hight_check(self.random_sleep)

                # xpath of movie urls

                for movie_url_tag in self.crawl_wrapper.find_info("xpath", movie_urls_collection_xpath,
                                                                  type_of_element='elements'):
                    movie_url = self.crawl_wrapper.get_href_value(movie_url_tag)
                    if movie_url == '':
                        continue
                    movie_urls.append(movie_url)

                    # self.logger.info('Find this movie url = ' + movie_url)
            # create input xlsx file
            movie_urls_df = self.create_input_xlsx_file(movie_urls,
                                                        self.utils.library_filename + self.movie_url_input_file_name)

            self.logger.info('Collected movie urls total : ' + str(len(movie_urls_df['urls'])),
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, self.movie_url_input_file_name)

            # self.logger.info('Xlsx file is generated include movie urls')

            # sending email alert for success of movie url collection
            self.utils.send_email_alert(self.utils.library_name, self.movie_url_input_file_name)
            # self.logger.info('Email alert sent for movie urls is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_url_extracting function: {e}", Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            self.utils.send_email_alert(self.utils.library_name, self.movie_url_input_file_name, e)
            sys.exit()

    def movie_data_extracting(self):
        """
                   Extracts data for movies from a list of URLs.

                   This method performs the following steps:
                   1. Checks if movie data extraction has already been completed by checking a flag.
                   2. Reads movie URLs from an Excel file.
                   3. Iterates through each movie URL to scrape relevant data points.
                   4. Constructs a DataFrame with the scraped data.
                   5. Appends the data to the DataFrame and updates the status in the input Excel file.
                   6. Sends an email alert upon successful completion.
                   7. Saves the final DataFrame to an Excel file.
                   8. Calls the 'Tv_season_url_extracting' function.

                   Raises:
                       Exception: If any error occurs during the movie data collection process.

                   Note:
                       This function relies on certain configurations specified in the 'movie_data_xpath' and 'DataFrameColumns' section of the config.
                       It uses the 'get_title_with_retry' method for retrieving movie titles and methods from the 'media_core' class
                       for logging, email alerts, and flag management.
                   """

        # region xpath of movie data collection
        genre_check_xpath= self.utils.xpaths_dict['check_movie_genre']
        year_check_xpath= self.utils.xpaths_dict['check_movie_year']
        duration_check_xpath= self.utils.xpaths_dict['check_movie_duration']
        rating_check_xpath= self.utils.xpaths_dict['check_movie_rating']
        movie_genre_xpath = self.utils.xpaths_dict['movie_genre']
        movie_year_xpath = self.utils.xpaths_dict['movie_year']
        movie_duration_xpath= self.utils.xpaths_dict['movie_duration']
        movie_rating_xpath= self.utils.xpaths_dict['movie_rating']
        movie_data_output_comon_file_name = 'Movies_data'
        self.movie_url_input_file_name = 'Movies_url'

        # end region
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, movie_data_output_comon_file_name):
                self.logger.info('Movie data collection already Finished',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            self.movie_final_data_list_of_dict = []

            # read input file for movie urls based on the condition.
            movie_input_url = self.input_filter_read_excel_file(
                excel_filename_path=self.utils.library_filename + self.movie_url_input_file_name)
            # Read columns from config
            movie_columns_str = self.utils.movie_columns
            movie_columns_list = movie_columns_str.split(',')

            # initialize dataframe with fixed column name for movie
            self.df_movies_schema = pd.DataFrame(columns=movie_columns_list)

            self.logger.info('Starting Movie data collection ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # loop every movie url for collect datapoints
            for index, row in movie_input_url.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                movie_url = row['urls']

                self.logger.info(f'Collecting movie data for url : {movie_url}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                self.crawl_wrapper.open_url(movie_url, self.random_sleep)
                title = month = day = currency = sdrent = sdbuy = hdrent = hdbuy = rating = mov_format = cast = year = duration = network = synopsis = director = production_company = writer = genre = ""

                soup = BeautifulSoup(self.crawl_wrapper.driver.page_source, 'html.parser')
                all_elements = soup.find_all("script")
                data = json.loads(all_elements[1].contents[0])
                try:
                    title = data['name']
                except:
                    title = ''
                if self.crawl_wrapper.driver.find_elements("xpath", "//dt[@class='typ-caption']")[0].text == 'Genre':
                    genre = self.crawl_wrapper.find_info("xpath",genre_check_xpath, type_of_element='elements')[0]

                    year = self.crawl_wrapper.find_info("xpath",year_check_xpath, type_of_element='elements')[1]

                    duration = self.crawl_wrapper.find_info("xpath", duration_check_xpath,type_of_element='elements')[2]

                    rating = self.crawl_wrapper.find_info("xpath", rating_check_xpath, type_of_element='elements')[3]
                else:
                    genre = self.crawl_wrapper.find_info("xpath",movie_genre_xpath,type_of_element='element')

                    year =self.crawl_wrapper.find_info("xpath",movie_year_xpath,type_of_element='element')
                    
                    duration = self.crawl_wrapper.find_info("xpath",movie_duration_xpath,type_of_element='element')

                    if 'hr' in duration:
                        duration = duration.replace('hr', '').replace("min", "").strip()
                        if ' ' in duration:
                            duration = duration.split('  ')
                            duration = int(duration[0]) * 60 + int(duration[1])

                        else:
                            duration = duration.replace("min", "")
                            duration = int(duration) * 60

                    rating = self.crawl_wrapper.find_info("xpath",movie_rating_xpath,type_of_element='element')
                try:
                    synopsis = data['description']
                except:
                    synopsis=""
                try:
                    act = []
                    for i in data['actor']:
                        act.append(i['name'])
                    cast = '|'.join(act)
                except:
                    cast = ''
                try:
                    dir = []
                    for i in data['director']:
                        dir.append(i['name'])

                    director = '|'.join(dir)
                except:
                    director = ''

                movie_final_data_dict = {'Content Type': 'Movie', 'Service': self.utils.library_name.split('_')[0],
                                         'Country': self.utils.library_instance.split('_')[-1],
                                         'Collection Date': self.utils.collectiondate, 'Title': title,'Year': year,
                                         'Month': month, 'Day': day, 'Rating': rating, 'Currency': currency,
                                         'Price SD Rent': sdrent, 'Price SD Buy': sdbuy, 'Price HD Rent': hdrent,
                                         'Price HD Buy': hdbuy, 'Genre': genre, 'Duration (minutes)': duration,
                                         'Network': network, 'Synopsis': synopsis, 'Language': 'English',
                                         'Production': production_company, 'Studio': '', 'Cast': cast,
                                         'Director': director, 'Writer': writer, 'Format': mov_format, 'URL': movie_url}
                # append data in list
                self.movie_final_data_list_of_dict.append(movie_final_data_dict)

                self.logger.info(f'Collected total movie data count : {str(len(self.movie_final_data_list_of_dict))}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                # update and save status in input file
                self.update_and_save_excel(index, movie_input_url, "status", "Done", self.utils.library_filename + self.movie_url_input_file_name)
            # make data file
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + movie_data_output_comon_file_name)
            # # send email alert for success of movie data collection
            self.utils.send_email_alert(self.utils.library_name, movie_data_output_comon_file_name)
            # self.logger.info('Email alert sent for movie data is completed')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, movie_data_output_comon_file_name)

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_data_extracting function: {e} for url : {movie_url}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            # make data file whatever is completed
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + movie_data_output_comon_file_name)
            self.utils.send_email_alert(self.utils.library_name, movie_data_output_comon_file_name, e)
            sys.exit()

    def Tv_show_url_extracting(self):
        base_series_url_xpath = self.utils.xpaths_dict['movie_url']
        genre_url_xpath = self.utils.xpaths_dict['tv_show_category']
        series_urls_collection_xpath = self.utils.xpaths_dict['movie_url_xpath']
        self.tv_show_url_input_file_name = 'Tv_shows_url'

        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name):
                self.logger.info('tv show url collection already Finished', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            series_links = []
            Genre_urls = []
            # logger.info function inserting logs in file

            self.logger.info("Starting tv show url collection ", Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # base_tv_show_url is where we are find series urls

            self.crawl_wrapper.open_url(base_series_url_xpath, self.random_sleep)
            # self.logger.info('Hit base tv show url')

            # self.logger.info('Start gather genre urls ----------------------------------')

            # xpath of getting genre urls

            for Genre_url_tag in self.crawl_wrapper.driver.find_elements("xpath", genre_url_xpath):
                Genre_url = self.crawl_wrapper.get_href_value(Genre_url_tag)
                if Genre_url == '':
                    continue
                Genre_urls.append(Genre_url)

                # self.logger.info('Find this genre url = ' + Genre_url)

            # collection of series urls from genre urls
            # self.logger.info('Start gather series urls ----------------------------------')
            for Genre in Genre_urls:
                self.crawl_wrapper.open_url(Genre, self.random_sleep)
                # self.logger.info('Hit this genre url  = ' + str(Genre_url))

                for series_url_tag in self.crawl_wrapper.find_info("xpath", series_urls_collection_xpath, type_of_element='elements'):
                    series_url = self.crawl_wrapper.get_href_value(series_url_tag)
                    # Use scrolling function of media wrap

                    self.crawl_wrapper.scrolling_page_with_hight_check(self.random_sleep)

                    if series_url and 'show' in series_url:
                        series_links.append(series_url)

                    # self.logger.info('Find this series url = ' + movie_url)
            # create input xlsx file

            series_urls_df = self.create_input_xlsx_file(series_links,self.utils.library_filename + self.tv_show_url_input_file_name)
            self.logger.info('Collected tv show urls total : ' + str(len(series_urls_df['urls'])),
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name)

            # self.logger.info('Xlsx file is generated include series urls')

            # sending email alert for success of tv show url collection
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name)
            # self.logger.info('Email alert sent for series urls is completed')

        except Exception as e:
            # exception for any error while collecting tv show url and send alert
            self.logger.error(f"Exception in tv_show_url_extracting function: {e}", Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name, e)
            sys.exit()

    def Tv_show_data_extracting(self):

        """
                    Extracts data for TV show episodes from given TV show season URLs.

                    This method performs the following steps:
                    1. Checks if TV show data extraction has already been completed by checking a flag.
                    2. Maximizes the window of the web driver.
                    3. Reads input data from an Excel file containing TV show season URLs.
                    4. Reads columns from the configuration file.
                    5. Initializes a dataframe with fixed column names for TV show data.
                    6. Loops through each TV show season URL to scrape data points for each episode.
                    7. Sends email alerts upon successful completion.
                    8. Saves the TV show data to an Excel file.
                    9. Sets a flag indicating that TV show data extraction has been completed.

                    Raises:
                        Exception: If any error occurs during the TV show data collection process.

                    Note:
                        This function relies on certain configurations specified in the 'tv_show_data_xpath' and 'DataFrameColumns' sections of the config.
                    """

        # region file names
        tv_show_data_output_comon_file_name = 'Tv_shows_data'
        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, tv_show_data_output_comon_file_name):
                self.logger.info('Tv show data collection already Finished', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return
            # append data in list
            self.tv_show_final_data_list_of_dict = []

            # read input file of tv show seasons urls with condition
            tv_show_input_url_df = self.input_filter_read_excel_file(excel_filename_path=self.utils.library_filename + self.tv_show_url_input_file_name)
            # Read columns from config
            tvshow_columns_str = self.utils.tvshows_columns
            tvshow_columns_list = tvshow_columns_str.split(',')

            # initialize dataframe with fixed coulmn name for movie
            self.df_tvshows_schema = pd.DataFrame(columns=tvshow_columns_list)

            self.logger.info('Starting Tv show data collection ', Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            # loop every season url

            for index, row in tv_show_input_url_df.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                # call parse tv show data function for collect tv show data
                self.parse_tv_show_data(index, row, tv_show_input_url_df)
                self.logger.info(
                    f'Collected total Tv Show data count : {str(len(self.tv_show_final_data_list_of_dict))}',
                    Process_id=f'{self.utils.ProcessID}',
                    library_instance=f'{self.utils.library_instance}',
                    Transaction_id=f'{self.utils.TransactionID}')

            # make data file
            self.create_output_xlsx_file(self.df_tvshows_schema, self.tv_show_final_data_list_of_dict,
                                         self.utils.library_filename + tv_show_data_output_comon_file_name)

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, tv_show_data_output_comon_file_name)

            # send email alert for success of tv show data collection
            self.utils.send_email_alert(self.utils.library_name, tv_show_data_output_comon_file_name)
            # self.logger.info('Email alert sent for tv show data is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in Tv_show_data_extracting function: {e} for url {row['urls']}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            # make data file whatever is done
            self.create_output_xlsx_file(self.df_tvshows_schema, self.tv_show_final_data_list_of_dict,
                                         self.utils.library_filename + tv_show_data_output_comon_file_name)
            self.utils.send_email_alert(self.utils.library_name, tv_show_data_output_comon_file_name, e)
            sys.exit()
    def parse_tv_show_data(self, index, row, series_input_url_df):

        # region tv show data xpath
        tv_show_title_xpath=self.utils.xpaths_dict['tv_show_title']
        tv_show_cast_xpath = self.utils.xpaths_dict['tv_show_cast']
        tv_show_synopsis_xpath = self.utils.xpaths_dict['tv-show_synopsis']
        tv_show_rating_xpath= self.utils.xpaths_dict['tv_show_rating']
        dropdown_click_xpath= self.utils.xpaths_dict['dropdown_click']
        season_click_xpath= self.utils.xpaths_dict['season_click']
        season_no_xpath=self.utils.xpaths_dict['season_no']
        scroll_button_xpath= self.utils.xpaths_dict['scroll_button']
        season_number_xpath=self.utils.xpaths_dict['season_number']
        genre_year_xpath= self.utils.xpaths_dict['tv_show_genre_year']
        each_season_click=self.utils.xpaths_dict['season']
        # end region

        tv_show_url = row['urls']

        self.crawl_wrapper.open_url(tv_show_url)

        self.title = self.month = self.day = self.year = self.currency = self.sdrent = self.sdbuy = self.hdrent = self.hdbuy = self.cast = self.director = self.genre = self.writer = self.synopsis = self.duration = self.rating = self.season_number = self.show_url = self.episode_number = self.episode_name = self.episode_Synopsis = self.no_epi = self.episode_url = ""

        self.logger.info(f'Collecting Tv Show Data for url : {tv_show_url}', Process_id=f'{self.utils.ProcessID}',
                         library_instance=f'{self.utils.library_instance}',
                         Transaction_id=f'{self.utils.TransactionID}')

        self.title = self.crawl_wrapper.find_info("xpath",tv_show_title_xpath,type_of_element='element')

        if self.title == '':
            self.logger.info('title not found')

        self.genre_year = self.crawl_wrapper.find_info("xpath",genre_year_xpath,type_of_element='elements')
        self.year= self.genre_year[1].text

        self.cast = self.crawl_wrapper.find_info("xpath",tv_show_cast_xpath,type_of_element='element')

        self.synopsis = self.crawl_wrapper.find_info("xpath",tv_show_synopsis_xpath,type_of_element='element')

        self.genre = self.genre_year[0].text

        self.rating = self.crawl_wrapper.driver.find_element("xpath",tv_show_rating_xpath).get_attribute("aria-label").replace("Rated", '').strip()
        self.show_url = self.crawl_wrapper.driver.current_url
        # scroll down with required position
        try:
            self.crawl_wrapper.scroll_to_position(20,550)
        except:
            pass

        # check dropdown exist or not
        try:
            self.crawl_wrapper.driver.find_element("xpath", self.utils.xpaths_dict['dropdown'])
            dropdown_select = True
        except:
            dropdown_select = False
        if dropdown_select:
            # click dropdown
            self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",dropdown_click_xpath,type_of_element='elements')[0])
            seasons = self.crawl_wrapper.driver.find_elements("xpath",self.utils.xpaths_dict['season'])
            self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",season_click_xpath,type_of_element='elements')[0])
            if len(seasons)>0:
                for seas in range(0,len(seasons)):
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",season_click_xpath,type_of_element='elements')[0])
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",each_season_click,type_of_element='elements')[seas])
                    self.season_number=self.crawl_wrapper.find_info("xpath",season_no_xpath,type_of_element='element').replace("Season","").strip()
                    # click season button
                    try:
                        for i in range(0,10):
                            self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",scroll_button_xpath,type_of_element='elements')[0])
                    except:
                        pass
                    # call episode function to fetch episode details
                    self.episodes()

                    self.update_and_save_excel(index, series_input_url_df, "status", "Done",self.utils.library_filename + self.tv_show_url_input_file_name)
        else:
            # for single season fetch details
            self.season_number = self.crawl_wrapper.find_info("xpath", season_number_xpath,type_of_element='element').replace("Season","").strip()
            try:
                for i in range(0, 10):
                    try:
                       self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",scroll_button_xpath,type_of_element='elements')[0])
                    except:
                        break
            except:
                pass
            # call episode function to fetch episode details

            self.episodes()
            self.update_and_save_excel(index, series_input_url_df, "status", "Done",
                                       self.utils.library_filename + self.tv_show_url_input_file_name)

    def episode_data(self,epi):
        epsisode_name_unique=[]

        episode_no_xpath = self.utils.xpaths_dict['episode_no']
        episode_name = self.utils.xpaths_dict['episode_name']
        episode_synopsis_xpath = self.utils.xpaths_dict['episode_synopsis']
        episode_duration_xpath = self.utils.xpaths_dict['episode_duration']
        episode_number_correct = []
        episode_number = self.crawl_wrapper.inner_element(epi, episode_no_xpath).replace("EPISODE", "").strip()

        episode_name = self.crawl_wrapper.inner_element(epi, episode_name)
        if episode_number in episode_number_correct:
            return

        # check episode already have or not for this season
        if episode_name in epsisode_name_unique:
            return
        epsisode_name_unique.append(episode_name)
        episode_number_correct.append(episode_number)

        episode_synopsis = self.crawl_wrapper.inner_element(epi, episode_synopsis_xpath)
        duration = self.crawl_wrapper.inner_element(epi, episode_duration_xpath).split("·")[0].replace("min", "").strip()

        if 'hr' in duration:
            duration = duration.replace('hr', '').strip()
            if ' ' in duration:
                duration = duration.split()
                duration = int(duration[0]) * 60 + int(duration[1])
            else:
                duration = int(duration) * 60
        tv_show_final_data_dict = {'Content Type': 'Tv Show',
                                   'Service': self.utils.library_instance.split('_')[0],
                                   'Country': self.utils.library_instance.split('_')[-1],
                                   'Collection Date': self.utils.collectiondate, 'Title': self.title,
                                   'Year': self.year, 'Month': '', 'Day': '',
                                   'Season Number': self.season_number,
                                   'Episode Number': episode_number, 'Episode Name': episode_name,
                                   'Number Episodes': '', 'Rating': self.rating, 'Currency': '',
                                   'Price SD Rent': '',
                                   'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                                   'Genres': self.genre, 'Duration (minutes)': duration,
                                   'Network': '', 'Synopsis': self.synopsis, 'Language': '',
                                   'Production Company': '', 'Studio': '', 'Cast': self.cast,
                                   'Director': self.director, 'Writer': '',
                                   'Format': '', 'Season URL': self.show_url,
                                   'Episode URL': self.episode_url,
                                   'Episode Synopsis': episode_synopsis}
        self.tv_show_final_data_list_of_dict.append(tv_show_final_data_dict)
    def episodes(self):
        episode_list_xpath = self.utils.xpaths_dict['episode_list']
        episode_list = self.crawl_wrapper.driver.find_elements("xpath", episode_list_xpath)
        for epi in episode_list:
            self.episode_data(epi)





