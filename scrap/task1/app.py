import logging
import pathlib
from pickle import FALSE
import re
import time
from dataclasses import dataclass
from typing import List, Dict
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from mashumaro import DataClassYAMLMixin, DataClassJSONMixin
from sqlalchemy import Column, String, Integer, JSON
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

logging.basicConfig(level=logging.INFO, format='%(name)s - %(message)s')

logger = logging.getLogger('#')


@dataclass
class Config(DataClassYAMLMixin):
    URLS_FILENAME: pathlib.Path


config = Config.from_yaml(open('config.yaml').read())

engine = create_engine('sqlite:///app.db', echo=False)
Base = declarative_base()
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)
session = Session()


class Item(Base):
    __tablename__ = 'items'

    id = Column(Integer, primary_key=True, autoincrement=True)  # id
    nog = Column(String)  # Nog
    night_country = Column(String)  # Night or Country
    name_social_reason = Column(String)  # Name of Socail reason
    types_products_offered = Column(String)  # Types of Products offered
    total_amount_offered = Column(String)  # Total amnount Offered(Q.)
    contest_description_link = Column(String)  # Link url
    page_data = Column(JSON)  # Page Data

    def __repr__(self):
        return f'Item(purchase_number={self.night_country}-{self.nog})'


Base.metadata.create_all(engine)


def get_soup(url: str, data: Optional[dict] = None) -> BeautifulSoup:
    if data is None:
        resp = requests.get(url)
    else:
        resp = requests.post(url, data=data)

    return BeautifulSoup(resp.text, 'lxml')


class NoNextPage(Exception):
    pass


@dataclass
class PageData(DataClassJSONMixin):
    nog: str
    description: str
    modality: str
    nit_country: str
    presentation_date: str
    opening_date: str
    authenticity_code: str
    bidders: List[Dict]


def get_page_data(url: str) -> PageData:
    soup = get_soup(url)
    soup = get_soup(url=url, data=dict(
        __EVENTTARGET='MasterGC$ContentBlockHolder$RadTabStrip1',
        __EVENTARGUMENT='{"type":0,"index":"3"}',
        __VIEWSTATE=soup.find('input', id='__VIEWSTATE').get('value'),
        __VIEWSTATEGENERATOR=soup.find('input', id='__VIEWSTATEGENERATOR').get('value'),
        MasterGC_ContentBlockHolder_RadTabStrip1_ClientState='{"selectedIndexes":["3"],'
                                                             '"logEntries":[],"scrollState":{}}'
    ))
    table_a = soup.find('div', id='MasterGC_ContentBlockHolder_DivContenido')
    pd = PageData(
        nog=table_a.find('td', text=re.compile('NOG:')).find_next_sibling('td').get_text(strip=True),
        description=table_a.find('td', text=re.compile('Descripción:')).find_next_sibling('td').get_text(strip=True),
        modality=table_a.find('td', text=re.compile('Modalidad:')).find_next_sibling('td').get_text(strip=True),
        nit_country=table_a.find('td', text=re.compile('NIT o país:')).find_next_sibling('td').get_text(strip=True),
        presentation_date=table_a.find('td', text=re.compile('Fecha de presentación:')).find_next_sibling('td').get_text(strip=True),
        opening_date=table_a.find('td', text=re.compile('Fecha de apertura:')).find_next_sibling('td').get_text(strip=True),
        authenticity_code=table_a.find('td', text=re.compile('Código de autenticidad:')).find_next_sibling('td').get_text(strip=True),
        bidders=[]
    )

    table_b = soup.find('table', id='MasterGC_ContentBlockHolder_grdTP')
    headers = [header.text for header in table_b.find('tr', class_="HeaderTablaDetalle").find_all('td')]
    for row in table_b.find_all('tr', class_='FilaTablaDetalle'):
        pd.bidders.append({headers[i]: cell.get_text(strip=True) for i, cell in enumerate(row.find_all('td'))})
    
    return pd


class App:
    def __init__(self, url: str):
        self.url: str = url
        self.logger = logging.getLogger('#')

    def save(self):
        count = 1
        soup = get_soup(url=self.url)

    # while soup is not None:
        table = soup.find('table', id='MasterGC_ContentBlockHolder_GrdListadoProductos')
        nog = soup.find('span', id='MasterGC_ContentBlockHolder_lblNOG').get_text(strip=True)
        # current_page = table.find('tr', class_='TablaPagineo').td.find_next('span').find_next('span')
        # page_no = current_page.get_text()
        # self.logger.info(f'### Processing Page: {page_no}')
        # time.sleep(60)

        rows = [row for row in table.find_all('tr', class_=['FilaTablaDetalle'])]

        self.logger.info(f'total row found: {len(rows)}')

        for row in rows:
            # time.sleep(3)
            td = row.find('td')
            # print(row)
            # print()
            # break
            item = Item(
                nog=nog,
                night_country=row.contents[1].get_text(strip=True),
                name_social_reason=row.contents[2].get_text(strip=True),
                types_products_offered=row.contents[3].get_text(strip=True),
                total_amount_offered=row.contents[4].get_text(
                    strip=True),
                contest_description_link=row.contents[4].find('a').get('href'),
            )
            item.contest_description_link = urljoin(self.url, item.contest_description_link)

            if session.query(Item).filter_by(night_country=item.night_country).first() is None:
                self.logger.info(f'writing {count} -> {item}')
                item.page_data = get_page_data(url=item.contest_description_link).to_dict()
                print(item)
                # break
                session.add(item)
                session.commit()

            else:
                self.logger.info(f'skipping {count} -> {item}')

            count += 1

        # nxt_lnk = current_page.find_next('a')
        # nxt_lnk = re.match(r"javascript:__doPostBack\('(.+)',''\)", nxt_lnk.get('href'))

        # if nxt_lnk is None:
        #     self.logger.info('no next page found')
        #     break

        # nxt_lnk = nxt_lnk.group(1)

        # form = soup.find('form', id='aspnetForm')
        # pd = {inp.get('name'): inp.get('value') for inp in form.find_all('input', value=True)}
        # pd['MasterGC$ContentBlockHolder$ScriptManager1'] = f'MasterGC$ContentBlockHolder$UpdatePanel1|{nxt_lnk}'
        # pd['__ASYNCPOST'] = True
        # pd['__EVENTTARGET'] = {nxt_lnk}

        # soup = get_soup(url=self.url, data=pd)


def main():
    urls: List[str] = [line.strip() for line in open(config.URLS_FILENAME).readlines() if line.strip()]

    for i, url in enumerate(urls):
        logger.info(f'[{i + 1}/{len(urls)}] {url}')
        app = App(url=url)
        app.save()


if __name__ == '__main__':
    main()
