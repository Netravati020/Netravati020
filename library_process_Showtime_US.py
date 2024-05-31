# -*- coding: utf-8 -*-
"""
objective = extract Movies and Tv Show urls and extracting required datapoints from Movies and Tv Show

Created on May 20 2024

@author: Netravati Madankar

"""
import sys, pandas as pd
from datetime import time
import time

from selenium.webdriver.common.by import By

from MediaVOD.library_processor.library_process_base import BaseCrawling
from WISE.wise_crawling_wrapper import Crawling_Wrap_selenium, Crawling_wrap_request
from selenium.webdriver import ActionChains

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

    def load_more_until_tag_found(self):
        load_more_button_xpath_xpath = self.utils.xpaths_dict['load_more_button']

        load_more_tag = True
        while load_more_tag:
            self.crawl_wrapper.one_time_scroll()
            try:
                if 'Load More' in str(self.crawl_wrapper.driver.page_source):
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",load_more_button_xpath_xpath,type_of_element='elements')[0])
                else:
                    load_more_tag = False
                    break
            except:
                break

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
        movie_urls_collection_xpath = self.utils.xpaths_dict['movie_url_collection']
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

            # xpath of movie urls
            self.load_more_until_tag_found()
            for movie_url_tag in self.crawl_wrapper.find_info("xpath", movie_urls_collection_xpath,type_of_element='elements'):
                movie_url = self.crawl_wrapper.get_href_value(movie_url_tag)
                if 'movies' in movie_url:
                    movie_urls.append(movie_url)

            # self.logger.info('Find this movie url = ' + movie_url)
            # create input xlsx file
            movie_urls_df = self.create_input_xlsx_file(movie_urls, self.utils.library_filename + self.movie_url_input_file_name)

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

        title_xpath = self.utils.xpaths_dict['movie_title']
        movie_meta_data_xpath = self.utils.xpaths_dict['meta_data']
        movie_synopsis_xpath = self.utils.xpaths_dict['movie_synopsis']

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
            movie_input_url=movie_input_url.head(5)
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

                title = month = day = currency = sdrent = sdbuy = hdrent = hdbuy = rating = mov_format = cast = year = duration = network = synopsis = director = studio = production_company = writer = genre = ""

                title = self.crawl_wrapper.driver.find_element("xpath", title_xpath).get_attribute('alt')

                # self.logger.info(' Title = ' + str(title))

                meta1 = self.crawl_wrapper.find_info("xpath", movie_meta_data_xpath, type_of_element='element')
                meta2 = meta1.strip().split('\n')
                genre = meta2[0]
                year = meta2[1]
                rating = meta2[2]

                duration = meta2[-1].replace("M", "")
                if 'H' in duration:
                    duration = duration.replace('H', '').strip()
                    if ' ' in duration:
                        duration = duration.split(' ')
                        duration = int(duration[0]) * 60 + int(duration[1])
                    else:
                        duration = int(duration) * 60

                synopsis = self.crawl_wrapper.find_info("css", movie_synopsis_xpath, type_of_element='element')

                # self.logger.info(' Synopsis = ' + str(synopsis))

                movie_final_data_dict = {'Content Type': 'Movie', 'Service': self.utils.library_instance.split('_')[0],
                                         'Country': self.utils.library_instance.split('_')[-1],
                                         'Collection Date': self.utils.collectiondate, 'Title': title, 'Year': year,
                                         'Month': month, 'Day': day, 'Rating': rating, 'Currency': currency,
                                         'Price SD Rent': sdrent, 'Price SD Buy': sdbuy, 'Price HD Rent': hdrent,
                                         'Price HD Buy': hdbuy, 'Genre': genre, 'Duration (minutes)': duration,
                                         'Network': network, 'Synopsis': synopsis, 'Language': 'English',
                                         'Production': production_company, 'Studio': studio, 'Cast': cast,
                                         'Director': director, 'Writer': writer, 'Format': mov_format, 'URL': movie_url}
                # append data in list
                self.movie_final_data_list_of_dict.append(movie_final_data_dict)

                self.logger.info(f'Collected total movie data count : {str(len(self.movie_final_data_list_of_dict))}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                # update and save status in input file
                self.update_and_save_excel(index, movie_input_url, "status", "Done",
                                           self.utils.library_filename + self.movie_url_input_file_name)
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
        base_series_url_xpath = self.utils.xpaths_dict['tv_show_url']
        series_urls_collection_xpath = self.utils.xpaths_dict['movie_url_collection']
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
            # logger.info function inserting logs in file

            self.logger.info("Starting tv show url collection ", Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # base_tv_show_url is where we are find series urls

            self.crawl_wrapper.open_url(base_series_url_xpath, self.random_sleep)
            # self.logger.info('Hit base tv show url')

            self.load_more_until_tag_found()
            for series_url_tag in self.crawl_wrapper.find_info("xpath", series_urls_collection_xpath,type_of_element='elements'):
                series_url = self.crawl_wrapper.get_href_value(series_url_tag)
                if 'shows' in series_url:
                    series_links.append(series_url)

            series_urls_df = self.create_input_xlsx_file(series_links, self.utils.library_filename + self.tv_show_url_input_file_name)
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
            self.logger.error(f"Exception in tv_show_url_extracting function: {e}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name, e)
            sys.exit()
    def Tv_show_data_extracting(self):
        pass