from database import Database
from html.parser import HTMLParser
import io
import logging
import pandas as pd
import requests
import zipfile

class Scraper(object):
    def __init__(self, hospital, page_url, link_text=None, link_title=None, base_url='', start_row=0, sheet_num=None):
        self.hospital = hospital
        self.page_url = page_url
        self.link_text = link_text or link_title
        self.is_title = link_text is None
        self.base_url = base_url
        self.start_row = start_row
        self.sheet_num = sheet_num
        self.db = Database(user='rachelwigell', database='pct')

    def scrape(self):
        """
        Hit the page url and download the chargemaster
        :return: a pandas dataframe containing the chargemaster data
        """

        page_data = requests.get(self.page_url)
        if page_data.status_code != 200:
            raise WebpageBadResponseException(self.hospital, page_data.status_code, page_data.content)

        chargemaster_url = self.parse_webpage(str(page_data.content), self.link_text, self.is_title)
        chargemaster_data = self.load_data_to_df(chargemaster_url)
        snake_case = [column.replace(' ', '_').lower() for column in chargemaster_data.columns]
        chargemaster_data.columns = snake_case
        self.write_data(chargemaster_data)

    def parse_webpage(self, page_data, text, is_title=False):
        """
        Parses a webpage's HTML to find the URL with link_text
        :param page_data: a string containing a webpage's HTML
        :return: The URL of the link on the page with the link_text
        """
        parser = WebpageParser()
        parser.feed(page_data)
        link = parser.url_text_pairs.get(text)
        if link is None:
            raise ChargemasterNotFoundException(self.hospital, text)
        else:
            return self.base_url + link

    def load_data_to_df(self, url):
        """
        Downloads chargemaster data from the given URL and loads it into a pandas dataframe
        :param url: The URL of the chargemaster
        :return: A pandas dataframe containing the chargemaster data
        """

        if url.endswith('.xlsx'):
            chargemaster_data = pd.DataFrame()
            try:
                sheets = pd.read_excel(url, sheet_name=None, header=self.start_row)
                for sheet_name, df in sheets.items():
                    df['sheet_name'] = sheet_name
                    chargemaster_data = chargemaster_data.append(df)
            except Exception as e:
                raise ChargemasterBadResponseException(hospital=self.hospital, pd_exception=e)

        elif url.endswith('.zip'):
            response = requests.get(url)
            if response.status_code == 200:
                archive = zipfile.ZipFile(io.BytesIO(response.content), 'r')
                try:
                    chargemaster_data = pd.read_excel(archive.open(archive.infolist()[0]),
                                                      sheet_name=self.sheet_num, header=self.start_row)
                except Exception as e:
                    raise ChargemasterBadResponseException(hospital=self.hospital, pd_exception=e)
            else:
                raise ChargemasterBadResponseException(hospital=self.hospital, status_code=response.status_code,
                                                       response_content=response.content)

        else:
            try:
                chargemaster_data = pd.read_csv(url, header=self.start_row)
            except Exception as e:
                raise ChargemasterBadResponseException(hospital=self.hospital, pd_exception=e)

        return chargemaster_data

    def write_data(self, data):
        """
        Writes chargemaster data to a database table
        :param data: a pandas dataframe containing the chargemaster data
        """
        self.db.create_table_from_dataframe('{hospital}_chargemaster'.format(hospital=self.hospital.lower()),
                                            data, schema_name='raw')


class WebpageParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.current_value = ''
        self.url_text_pairs = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            attrs_dict = dict(attrs)
            if 'title' in attrs_dict.keys():
                self.url_text_pairs[attrs_dict['title']] = attrs_dict['href']
            else:
                for name, value in attrs:
                    if name == "href":
                        self.current_value = value

    def handle_endtag(self, tag):
        self.current_value = ''

    def handle_data(self, data):
        self.url_text_pairs[data.strip()] = self.current_value
        self.current_value = ''



class WebpageBadResponseException(Exception):
    """
    Thrown when a hospital's webpage doesn't return a 200 status code
    """
    def __init__(self, hospital, status_code, response_content):
        logging.error("""
            {hospital} webpage did not load successfully.
            status code was: {status_code}.
            details: {response_content}
        """.format(hospital=hospital, status_code=status_code, response_content=response_content))


class ChargemasterNotFoundException(Exception):
    """
    Thrown when a link with link_text isn't found on the hospital's webpage.
    """
    def __init__(self, hospital, link_text):
        logging.error("""
            The chargemaster for {hospital} could not be downloaded from the webpage.
            The link text/title {link_text} could not be found.
        """.format(hospital=hospital, link_text=link_text))


class ChargemasterBadResponseException(Exception):
    """
    Thrown when a hospital's chargemaster URL doesn't return a 200 status code,
    or when a pandas read_csv or read_excel function fails
    """
    def __init__(self, hospital, status_code=None, response_content=None, pd_exception=None):
        if pd_exception:
            logging.error("""
                Error while downloading {hospital} chargemaster. The error that was raised is as follows:
            """.format(hospital=hospital))
            raise pd_exception
        else:
            logging.error("""
                {hospital} chargemaster did not download successfully.
                status code was: {status_code}.
                details: {response_content}
            """.format(hospital=hospital, status_code=status_code, response_content=response_content))
