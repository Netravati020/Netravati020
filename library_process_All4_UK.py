# -*- coding: utf-8 -*-
"""
objective = extract Movies and Tv Show urls and extracting required datapoints from Movies and Tv Show

Created on May 15 2024

@author: Netravati Madankar

"""
import sys, pandas as pd
from MediaVOD.library_processor.library_process_base import BaseCrawling
from WISE.wise_crawling_wrapper import Crawling_Wrap_selenium, Crawling_wrap_request
import re
import datetime


class process(BaseCrawling):
    def __init__(self, utils):
        super().__init__(utils)

        # use selenium class from wrapper and initialize_chrome_driver
        self.crawl_wrapper = Crawling_Wrap_selenium(self.utils)
        self.crawl_wrapper.initialize_chrome_driver()

        # put sleep that you observed in website
        self.random_sleep = self.crawl_wrapper.get_random_number(9, 12)
        self.crawl_wrapper_req= Crawling_wrap_request(self.utils)

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
        film_button_xpath_xpath = self.utils.xpaths_dict['films_button']
        movie_urls_collection_xpath = self.utils.xpaths_dict['movie_collection']
        accept_cookies_xpath= self.utils.xpaths_dict['accept_cookies']

        self.movie_url_input_file_name = 'Movies_url'

        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.movie_url_input_file_name):
                self.logger.info('Movie url collection already Finished',Process_id=f'{self.utils.ProcessID}',
                library_instance=f'{self.utils.library_instance}',
                Transaction_id=f'{self.utils.TransactionID}')
                return

            movie_urls = []

            # logger.info function inserting logs in file

            self.logger.info("Starting Movie url collection ",Process_id=f'{self.utils.ProcessID}',
                library_instance=f'{self.utils.library_instance}',
                Transaction_id=f'{self.utils.TransactionID}')

            # base_movie_url is where we are find movies urls

            self.crawl_wrapper.open_url(base_movie_url_xpath, self.random_sleep)
            # self.logger.info('Hit base movie url = ' + base_movie_url_xpath)
            # click accept cookies button
            self.crawl_wrapper.accept_cookie(accept_cookies_xpath)

            # collection of genre_urls
            for i in range(0,10):
                for i in range(0,10):
                    self.crawl_wrapper.scroll_with_hight(2000)

                self.crawl_wrapper.scroll_with_hight(2000)
                film_button_tags = self.crawl_wrapper.find_info("xpath", film_button_xpath_xpath,type_of_element='elements')
                if film_button_tags:
                    self.crawl_wrapper.click(film_button_tags[0])

            # xpath of movie urls

            for movie_url_tag in self.crawl_wrapper.find_info("xpath", movie_urls_collection_xpath,
                                                              type_of_element='elements'):
                movie_url = self.crawl_wrapper.get_href_value(movie_url_tag)
                if movie_url == '':
                    continue
                movie_urls.append(movie_url)

            # self.logger.info('Find this movie url = ' + movie_url)
            # create input xlsx file
            movie_urls_df = self.create_input_xlsx_file(movie_urls, self.utils.library_filename + self.movie_url_input_file_name)

            self.logger.info('Collected movie urls total : ' + str(len(movie_urls_df['urls'])),Process_id=f'{self.utils.ProcessID}',
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
            self.logger.error(f"Exception in movie_url_extracting function: {e}",Process_id=f'{self.utils.ProcessID}',
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


        movie_synopsis_xpath = self.utils.xpaths_dict['movie_synopsis']
        movie_duration_xpath = self.utils.xpaths_dict['movie_duration']
        accept_cookies_xpath= self.utils.xpaths_dict['accept_cookies']

        movie_data_output_comon_file_name = 'Movies_data'

        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, movie_data_output_comon_file_name):
                self.logger.info('Movie data collection already Finished', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            self.movie_final_data_list_of_dict = []

            # read input file for movie urls based on the condition.
            movie_input_url = self.input_filter_read_excel_file(
                excel_filename_path=self.utils.library_filename + self.movie_url_input_file_name)
            # movie_input_url=movie_input_url.head(5)
            # Read columns from config
            movie_columns_str = self.utils.movie_columns
            movie_columns_list = movie_columns_str.split(',')

            # initialize dataframe with fixed column name for movie
            self.df_movies_schema = pd.DataFrame(columns=movie_columns_list)

            self.logger.info('Starting Movie data collection ', Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # loop every movie url for collect datapoints
            for index, row in movie_input_url.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                movie_url = row['urls']

                self.logger.info(f'Collecting movie data for url : {movie_url}', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                self.crawl_wrapper.open_url(movie_url, self.random_sleep)
                self.crawl_wrapper.accept_cookie(accept_cookies_xpath)

                title = month = day = currency = sdrent = sdbuy = hdrent = hdbuy = rating = mov_format = cast = year = duration = network = synopsis =studio= director = production_company = writer = genre = ""
                # response = requests.get(url)
                soup = self.crawl_wrapper_req.BeautifulSoup_covert(self.crawl_wrapper.driver.page_source)
                title = soup.find('title').text.split('|')[0].replace("Watch","")

                # self.logger.info(' Title = ' + str(title))
                synopsis = self.crawl_wrapper.find_info("xpath", movie_synopsis_xpath, type_of_element='element')
                # self.logger.info(' Synopsis = ' + str(synopsis))

                matches = re.findall(r'\((\d{4})\)', synopsis)
                if matches:
                    year = matches[0]
                else:
                    year=""
                # self.logger.info(' Year = ' + str(year))

                duration_get = self.crawl_wrapper.find_info("xpath", movie_duration_xpath, type_of_element='element')
                duration=duration_get.split(" | ")[2].replace('mins', '')
                genre = duration_get.split(" | ")[0]

                # self.logger.info(' Duration = ' + str(duration))

                movie_final_data_dict = {'Content Type': 'Movie', 'Service': self.utils.library_instance.split('_')[0],
                                         'Country': self.utils.library_instance.split('_')[-1],
                                         'Collection Date': self.utils.collectiondate, 'Title': title, 'Year': year,
                                         'Month': month, 'Day': day, 'Rating': rating, 'Currency': currency,
                                         'Price SD Rent': sdrent, 'Price SD Buy': sdbuy, 'Price HD Rent': hdrent,
                                         'Price HD Buy': hdbuy, 'Genre': genre, 'Duration (minutes)': duration,
                                         'Network': network, 'Synopsis': synopsis, 'Language': 'English',
                                         'Production': production_company, 'Studio':studio, 'Cast': cast,
                                         'Director': director, 'Writer': writer, 'Format': mov_format, 'URL': movie_url}
                # append data in list
                self.movie_final_data_list_of_dict.append(movie_final_data_dict)

                self.logger.info(f'Collected total movie data count : {str(len(self.movie_final_data_list_of_dict))}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                # update and save status in input file
                self.update_and_save_excel(index, movie_input_url, "status", "Done",self.utils.library_filename + self.movie_url_input_file_name)
            # make data file
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + movie_data_output_comon_file_name)
            # send email alert for success of movie data collection
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
        """
                    Extracts TV show URLs and season URLs from a base TV show URL.

                    This method performs the following steps:
                    1. Checks if TV show URL extraction has already been completed by checking a flag.
                    2. Gathers genre URLs for TV shows from the base TV show URL.
                    3. Iterates through each genre URL to collect TV show URLs.
                    4. Iterates through each TV show URL to collect season URLs.
                    5. Removes duplicate TV show URLs and saves the season URLs to an Excel file.
                    6. Sends an email alert upon successful completion.
                    7. Calls the 'Tv_show_data_extracting' function.

                    Raises:
                        Exception: If any error occurs during the TV show URL and season URL collection process.

                    Note:
                        This function relies on certain configurations specified in the 'url_path' section of the config.
                        It uses methods from the 'media_core' class for logging, email alerts, and flag management.
                    """

        # region xpath of tv show url collection

        base_tv_show_url = self.utils.xpaths_dict['tv_show_url']
        show_more_button_xpath = self.utils.xpaths_dict['show_more_button']
        tv_show_url_collection_xpath = self.utils.xpaths_dict['tv_show_url_collection']
        accept_cookies_xpath= self.utils.xpaths_dict['accept_cookies']
        self.tv_show_url_input_file_name = 'Tv_shows_url'

        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name):
                self.logger.info('Tv show url collection already Finished', Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            tv_show_urls = []

            # logger.info function inserting logs in file

            self.logger.info('Starting Tv Show url Collection ', Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # base_tv_show_url is where we are find tv show urls

            self.crawl_wrapper.open_url(base_tv_show_url, self.random_sleep)
            # self.logger.info('Hit base tv show url = ' + base_tv_show_url)
            # click accept cookies button
            self.crawl_wrapper.accept_cookie(accept_cookies_xpath)

            def load_page():
                for i in range(3):
                    self.crawl_wrapper.scroll_with_hight(2000)
            # click show more button to load more content

            next_page=True
            while next_page:
                load_page()
                if 'all4-secondary-button all4-typography-body' in str(self.crawl_wrapper.driver.page_source):
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",show_more_button_xpath, type_of_element='elements')[0])
                else:
                    next_page=False

            # start collecting tv shows urls
            for tv_show_url_tag in self.crawl_wrapper.find_info("xpath",tv_show_url_collection_xpath,type_of_element='elements'):
                tv_show_url=self.crawl_wrapper.get_href_value(tv_show_url_tag)
                tv_show_urls.append(tv_show_url)


            # create input xlsx file
            tv_show_urls_df = self.create_input_xlsx_file(tv_show_urls,self.utils.library_filename + self.tv_show_url_input_file_name)

            self.logger.info(f'total tv show season urls we got is : {str(len(tv_show_urls_df["urls"]))}',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            # self.logger.info('Xlsx file is generated include seasons urls')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name)

            # send email alert for success of tv show url collection
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name)
            # self.logger.info('Email alert sent for tv show  urls is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in Tv_show_url_extracting function: {e}",
                              Process_id=f'{self.utils.ProcessID}',
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
                                It uses methods from the 'media_core' class for logging, email alerts, and flag management.
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
            series_input_url_df = self.input_filter_read_excel_file(
                excel_filename_path=self.utils.library_filename + self.tv_show_url_input_file_name)
            # series_input_url_df=series_input_url_df.head(5)
            # Read columns from config
            tvshow_columns_str = self.utils.tvshows_columns
            tvshow_columns_list = tvshow_columns_str.split(',')

            # initialize dataframe with fixed coulmn name for movie
            self.df_tvshows_schema = pd.DataFrame(columns=tvshow_columns_list)

            self.logger.info('Starting Tv show data collection ', Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            # loop every season url
            for index, row in series_input_url_df.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                # call parse tv show data function for collect tv show data
                self.parse_tv_show_data(index, row, series_input_url_df)
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

    def load_page(self):
        for i in range(3):
            self.crawl_wrapper.scroll_with_hight(2000)

     # click show more button to load more content
    def extractor(self):
        show_more_button_xpath = self.utils.xpaths_dict['show_more_button']

        next_page = True
        while next_page:
            self.load_page()
            if 'all4-secondary-button all4-typography-body' in str(self.crawl_wrapper.driver.page_source):
                self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", show_more_button_xpath, type_of_element='elements')[0])
            else:
                next_page = False

    def parse_tv_show_data(self, index, row, series_input_url_df):

        # region tv show data xpath
        seasons_xpath = self.utils.xpaths_dict['seasons']
        season_menu_click_xpath = self.utils.xpaths_dict['season_menu']
        each_season_click_xpath = self.utils.xpaths_dict['season_click']
        accept_cookies_xpath= self.utils.xpaths_dict['accept_cookies']

        # end region

        tv_show_url = row['urls']

        self.crawl_wrapper.open_url(tv_show_url)
        self.crawl_wrapper.accept_cookie(accept_cookies_xpath)

        self.title = self.month_digit = self.day_digit = self.year_digit = self.currency = self.sdrent = self.sdbuy = self.hdrent = self.hdbuy = self.studio = self.cast = self.director = self.genre = self.writer = self.synopsis = self.duration = self.rating = self.season_no = self.show_url = self.Episode_no = self.Episode_name = self.Episode_Synopsis = self.no_epi = self.episode_url = ""

        self.logger.info(f'Collecting Tv Show Data for url : {tv_show_url}', Process_id=f'{self.utils.ProcessID}',
                         library_instance=f'{self.utils.library_instance}',
                         Transaction_id=f'{self.utils.TransactionID}')

        # collect episode data
        self.episodes()
        seasons = self.crawl_wrapper.find_info("xpath", seasons_xpath, type_of_element='elements')
        for seasn in range(0, len(seasons)):
            # click season menu
            self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", season_menu_click_xpath, type_of_element='elements')[0])
            # click each season button
            self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", each_season_click_xpath, type_of_element='elements')[seasn])
            # click show more button to load more content
            for i in range(0, 5):
                self.extractor()
            # start collecting episode details
            self.episodes()

        self.update_and_save_excel(index, series_input_url_df, "status", "Done",self.utils.library_filename + self.tv_show_url_input_file_name)

    def parse_episode_data(self, epi_data):

        # region episode data xpath

        episode_name_xpath = self.utils.xpaths_dict['episode_name']
        episode_synopsis_xpath = self.utils.xpaths_dict['episode_synopsis']
        episode_duration_xpath = self.utils.xpaths_dict['episode_duration']
        episode_year_xpath = self.utils.xpaths_dict['episode_year']
        tv_show_synopsis_xpath = self.utils.xpaths_dict['tv_show_synopsis']
        tv_show_genre_xpath = self.utils.xpaths_dict['tv_show_genre']
        season_no_xpath = self.utils.xpaths_dict['season_no']
        # end region

        episode_name = self.crawl_wrapper.inner_element(epi_data, episode_name_xpath, locator='css')

        episode_Synopsis = self.crawl_wrapper.inner_element(epi_data, episode_synopsis_xpath, locator='css')

        ep_dur = self.crawl_wrapper.inner_element(epi_data, episode_duration_xpath, locator='css')
        try:
            dur = ep_dur.split('|')[0]
            Dur = ep_dur.split('|')[1]
        except:
            pass
        if 'mins' in dur:
            dura = dur.replace(' mins', '')
        else:
            dura = Dur.replace(' mins', '')
        duration = dura

        try:
            episode_year = self.crawl_wrapper.inner_element(epi_data, episode_year_xpath)
            episode_year=episode_year.split("|")[0].strip().replace('First shown:', '').strip()
            if '|' in episode_year:
                try:
                    year_digit = re.findall('(\d{4})', episode_year)[0]
                except:
                    year_digit = ''
                episode_year = episode_year.split('|')[0]
                date_object = datetime.datetime.strptime(episode_year, "%a %d %b")

                month_digit = date_object.month
                day_digit = date_object.day
                year_digit = ''
            else:
                date_object = datetime.datetime.strptime(episode_year, "%a %d %b %Y")
                month_digit = date_object.month
                day_digit = date_object.day
                year_digit = date_object.year
        except:
            month_digit = ''
            day_digit = ''
            year_digit = ''
            episode_year = ""
        soup = self.crawl_wrapper_req.BeautifulSoup_covert(self.crawl_wrapper.driver.page_source)
        title = soup.find('title').text.split('|')[0].replace('Watch ', '')
        synopsis = self.crawl_wrapper.find_info("xpath", tv_show_synopsis_xpath, type_of_element='element')
        genre = self.crawl_wrapper.find_info("xpath", tv_show_genre_xpath, type_of_element='element')
        if '|' in genre:
            genre=genre.split("|")[0]
        else:
            genre=self.crawl_wrapper.find_info("xpath", tv_show_genre_xpath, type_of_element='element')
        show_url = self.crawl_wrapper.driver.current_url
        season_no = self.crawl_wrapper.find_info("xpath", season_no_xpath, type_of_element='element')

        tv_show_final_data_dict = {'Content Type': 'Tv Show',
                                   'Service': self.utils.library_instance.split('_')[0],
                                   'Country': self.utils.library_instance.split('_')[-1],
                                   'Collection Date': self.utils.collectiondate, 'Title': title,
                                   'Year': year_digit, 'Month': month_digit, 'Day': day_digit,
                                   'Season Number': season_no,
                                   'Episode Number': '', 'Episode Name': episode_name,
                                   'Number Episodes': '', 'Rating': self.rating, 'Currency': '',
                                   'Price SD Rent': '',
                                   'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                                   'Genres': genre, 'Duration (minutes)': duration,
                                   'Network': '', 'Synopsis': synopsis, 'Language': 'English',
                                   'Production Company': '', 'Studio': self.studio, 'Cast': self.cast,
                                   'Director': self.director,
                                   'Writer': self.writer,
                                   'Format': '', 'Season URL': show_url, 'Episode URL': '',
                                   'Episode Synopsis': episode_Synopsis}
        # append data in list
        self.tv_show_final_data_list_of_dict.append(tv_show_final_data_dict)

    def episodes(self):

        # region episodes find xpath
        episodes_data_xpath = self.utils.xpaths_dict['episode_data']
        # end region
        self.Episodes_Data = self.crawl_wrapper.find_info("xpath", episodes_data_xpath, type_of_element='elements')
        for epi_data in self.Episodes_Data:
            self.parse_episode_data(epi_data)

