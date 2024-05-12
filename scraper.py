from abc import ABC, abstractmethod, abstractproperty
from typing import Optional
from bs4 import BeautifulSoup
from bs4.element import ResultSet
import pandas as pd
import time
from urllib.request import urlopen
import uuid
import re
from datetime import date
from dotenv import load_dotenv
import os
from collections import namedtuple
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    TimeoutException
)

from  src.infra.data_loader import DataLoader


ArticleData = namedtuple(
    'article_data', ['article_hash', 'uuid', 'title', 'publish_date', 'scrape_date',
                     'domain', 'news_url', 'article_url']
)
load_dotenv()


class Scraper(ABC):
    domain = ''
    address = ''
    name = ''
    headlines_table = "pymes_news_headlines"

    def __init__(self) -> None:
        self.url = self.domain + self.address
        self.scrape_date = date.today()
        self.webdriver = webdriver.Chrome(options=self.chrome_options)
        self.data_loader = DataLoader.create(os.environ["data_loader"])

    # main methods
    def scrape(self, no_earlier_than: Optional[date]=None) -> pd.DataFrame:
        cards = self.get_soup_cards(no_earlier_than)
        return self.create_newslist(cards)
    
    def update_newslist_data(self, no_earlier_than: Optional[date]=None) -> None:
        no_earlier_than = self.get_earliest_date_for_update(no_earlier_than)       
        data = self.scrape(no_earlier_than)
        data = data[~data.index.duplicated(keep='first')].copy() # TODO doe it still happen (????)
        self.data_loader.store_table(data, self.headlines_table)
    
    # helpers
    @property
    def chrome_options(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_argument("--incognito")
        return chrome_options

    @staticmethod
    def create(name) -> "Scraper":
        if name == 'ElPeriodico':
            return ElPeriodicoScraper()
        if name == 'Regio7':
            return Regio7Scraper()

    def get_soup_cards(self, no_earlier_than: Optional[date]=None) -> ResultSet:
        """
        :param tag: html tag, to search in html
        """
        x_path_accept = "/html/body/div[1]/div/div/div/div/div/div[2]/button[2]"
        
        self.webdriver.maximize_window()
        self.webdriver.get(self.url)
        time.sleep(5)
        accept_button = self.webdriver.find_element(By.XPATH, x_path_accept)

        accept_button.click()
        cards = self.open_all_cards(no_earlier_than)
        return cards

    def create_newslist(self, soup_cards):
        articles_data = []
        for card in soup_cards:
            articles_data.append(self.get_data_from_card(card))
        data = pd.DataFrame(articles_data).dropna(thresh=6)
        data.set_index("article_hash", inplace=True)
        return data

    @staticmethod
    def get_url_section(url_page, sect_start_sep, sect_end_sep):
        """
        :param url_page: the whole http
        :param domain: com, ru, cat and so on
        """
        url_reg = re.search(sect_start_sep + "(.+?)" + sect_end_sep, url_page)
        if url_reg:
            url = url_reg.group(1)
        return url
    
    def get_earliest_date_for_update(self, no_earlier_than: Optional[date]=None):
        old_data = self.data_loader.read_table(
            self.headlines_table,
            query=f"select publish_date from data where domain='{self.domain}'"
        )
        if not old_data.empty:
            latest_date = pd.to_datetime(old_data.publish_date.dropna().max()).date()
            if not no_earlier_than:
                return latest_date
            return max(no_earlier_than, latest_date)
        
    def stop_scraping(self, cards, no_earlier_than):
        if not no_earlier_than:
            return False
        earliest_card = min(
            self.get_card_date(card) 
            for card in cards 
            if self.get_card_date(card) is not pd.NaT
        )
        return earliest_card < no_earlier_than
                        

    @abstractproperty
    def soup_tags(self):
        ...

    @abstractmethod
    def open_all_cards(self, no_earlier_than: Optional[date]=None):
        ...

    @abstractmethod
    def get_data_from_card(self, card):
        ...


    @abstractmethod
    def get_text_from_card(self, html):
          ...

    @abstractmethod
    def get_snippet_from_card(self, html):
          ...


class ElPeriodicoScraper(Scraper):
    language = 'spanish'
    domain = 'https://www.elperiodico.com'
    address = '/es/temas/pymes-42198'
    website = domain + address
    name = 'ElPeriodico'

    def __init__(self) -> None:
        super().__init__()
        self.webdriver.set_page_load_timeout(15)

    @property
    def soup_tags(self):
        return {
            'name': 'div',
            'class': 'item'
        }

    def open_all_cards(self, no_earlier_than):
        path_to_button = "/html/body/main/div[2]/section/div/div[1]/div[2]/section/div/div[2]/div/a"
        page = 0
        while True:
            more_button = self.webdriver.find_element(By.XPATH, path_to_button)
            page += 1
            print(f"{page=}")
            try:
                action = ActionChains(self.webdriver)
                action.move_to_element(more_button)
                element = WebDriverWait(self.webdriver, 10).until(EC.element_to_be_clickable(more_button))
                element.click()
                time.sleep(1) # TODO remove, replace by a more sophisticated function
                html = self.webdriver.page_source
                soup = BeautifulSoup(html, "lxml")
                cards = soup.find_all(**self.soup_tags)
                if self.stop_scraping(cards, no_earlier_than):
                    break

            except ElementClickInterceptedException:
                break
            except TimeoutException:
                # TODO we need a normal logger here
                print(f"WARNING: {self.domain} returned TimeoutException")
                break
        return cards

    @staticmethod
    def get_card_date(card):
        publish_date = card.find("span", class_="location")
        if publish_date:
            return pd.to_datetime(publish_date.text).date()
        return pd.NaT

    def get_data_from_card(self, card):
        text = card.h2  # title of news in h2 in elperiodico
        publish_date = self.get_card_date(card)
        if text and (publish_date is not pd.NaT):
            article_url = self.domain + self.get_url_section(text.a['href'], "com", "$")
            return ArticleData( 
                article_hash=hash(f"{article_url}"),
                uuid=str(uuid.uuid4()),
                title=text.a['title'],
                publish_date=publish_date,
                scrape_date=self.scrape_date.strftime("%m/%d/%Y"),
                domain=self.domain,
                news_url=self.address,
                article_url=article_url,
            )

    def get_snippet_from_card(self, card):
        link = self.domain + self.get_url_section(card.a['href'], "com", "$")
        soup = BeautifulSoup(urlopen(link), "lxml")
        snippet = soup.find("h2", class_="subtitle")
        return snippet

    def get_text_from_card(self, card):
        link = self.domain + self.get_url_section(card.a['href'], "com", "$")
        soup = BeautifulSoup(urlopen(link), "lxml")
        articles_text = []
        tags = soup.find_all("p")
        tags = [i for i in tags if not i.attrs]
        tags_text = [i.text for i in tags]
        del tags_text[0:2]
        articles_text.append(tags_text)
        return articles_text


class Regio7Scraper(Scraper):
    language = 'catalan'
    domain = 'https://www.regio7.cat'
    address = '/economia/'
    name = 'Regio7'

    def __init__(self) -> None:
        super().__init__()

    @property
    def soup_tags(self):
        return {
            'name': 'a',
            'href': self.economia,
            'class_': "new__media"
        }

    @staticmethod
    def economia(href):
        return href and re.compile("economia").search(href)

    def open_all_cards(self, no_earlier_than):
        page=0
        for i in range(39): # TODO replace with data limiting
        #while True:
            page=page+1
            print(page)
            path_to_button = "btn-view-more"
            more_button = self.webdriver.find_element(By.CLASS_NAME, path_to_button)
            try:
                self.webdriver.execute_script("arguments[0].scrollIntoView();", more_button)
                more_button.click()
                self.webdriver.implicitly_wait(1)
                html = self.webdriver.page_source
                soup = BeautifulSoup(html, "lxml")
                cards = soup.find_all(**self.soup_tags)
                if self.stop_scraping(cards, no_earlier_than):
                    break
            except ElementClickInterceptedException:
                break
            except TimeoutException:
                # TODO we need a normal logger here
                print(f"WARNING: {self.domain} returned TimeoutException")
                break
        return cards
    
    def get_card_date(self, card):
        publish_date = '-'.join(card['href'].replace(self.address, '').split('/')[0:3])
        if publish_date:
            try:
                return pd.to_datetime(publish_date).date()
            except:
                return pd.NaT
        return pd.NaT

    def get_data_from_card(self, card):
        article_url = self.domain + card['href']
        publish_date = self.get_card_date(card)
        if publish_date is not pd.NaT:
            return ArticleData(
                article_hash=hash(f"{article_url}"),
                uuid=str(uuid.uuid4()),
                title=card['title'],
                publish_date=publish_date,
                scrape_date=self.scrape_date,
                domain=self.domain,
                news_url=self.address,
                article_url=article_url,
            )

    def get_snippet_from_card(self, card):
        url = self.domain + card['href']
        soup = BeautifulSoup(urlopen(url), "lxml")
        snippet = soup.find("h1", class_="h1 headline-article__head")
        return snippet

    def get_text_from_card(self, card):
        link = card.a.get("href")
        url = "https://www.regio7.cat" + link
        soup = BeautifulSoup(urlopen(url), "lxml")
        tags = soup.find("div", class_="bbnx-module")
        ps = tags.find_all(["p", "h2"])
        articles_text = []
        for i in ps:
            articles_text.append(i.text)
        return articles_text


