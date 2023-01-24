import logging
import pathlib
import re
import time
from dataclasses import dataclass
from typing import List, Dict
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from mashumaro import DataClassYAMLMixin, DataClassJSONMixin
from sqlalchemy import Column, String, JSON
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

    operation_number = Column(String, primary_key=True)  # NOG (Número de Operación Guatecompras)
    publication_date = Column(String)  # Fecha de publicación
    deadline_to_bid = Column(String)  # Fecha límite para ofertar
    can_participate = Column(String)  # Puede participar cualquier proveedor
    contest_status = Column(String)  # Estatus del concurso
    publishing_entity = Column(String)  # Entidad que publica
    buying_unit = Column(String)  # Unidad compradora que publica
    contest_category = Column(String)  # Categoría del concurso
    contest_description = Column(String)  # Descripción del concurso
    contest_description_link = Column(String)  # Descripción del concurso
    modality = Column(String)  # Modalidad
    page_data = Column(JSON)  # Modalidad

    def __repr__(self):
        return f'Item(purchase_number={self.operation_number})'


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
    entity: str
    closing_date: str

    bidders: List[Dict]
    suppliers: List[Dict]
    total: str


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
    table_a = soup.find('div', id='MasterGC_ContentBlockHolder_WUCDetalleConcurso_divDetalleConcurso')
    pd = PageData(
        nog=table_a.find('div', text=re.compile('NOG:')).find_next_sibling('div').get_text(strip=True),
        description=table_a.find('div', text=re.compile('Descripción:')).find_next_sibling('div').get_text(strip=True),
        modality=table_a.find('div', text=re.compile('Modalidad:')).find_next_sibling('div').get_text(strip=True),
        entity=table_a.find('div', text=re.compile('Entidad:')).find_next_sibling('div').get_text(strip=True),
        closing_date='',
        bidders=[],
        suppliers=[],
        total=''
    )

    if closing_date := table_a.find('div', text=re.compile('Fecha de cierre de recepción de ofertas:')):
        pd.closing_date = closing_date.find_next_sibling('div').get_text(strip=True)

    table_b = soup.find('table', id='MasterGC_ContentBlockHolder_wcuConsultaConcursoAdjudicaciones_gvOfertas')
    headers = [header.text for header in table_b.find_all('th')]
    for row in table_b.find_all('tr', class_='FilaTablaDetalle'):
        pd.bidders.append({headers[i]: cell.get_text(strip=True) for i, cell in enumerate(row.find_all('td'))})

    table_c = soup.find('div', id='MasterGC_ContentBlockHolder_wcuConsultaConcursoAdjudicaciones_divDetalleFacturas')
    headers = [header.text for header in table_c.find_all('th')]
    for row in table_c.find('div', class_='accordionCabecera').find_all('tr'):
        pd.suppliers.append({headers[i]: cell.get_text(strip=True) for i, cell in enumerate(row.find_all('td')[1:])})

    total = soup.find('span', id='MasterGC_ContentBlockHolder_wcuConsultaConcursoAdjudicaciones_lblMontoTotal')
    pd.total = total.get_text(strip=True)

    return pd


class App:
    def __init__(self, url: str):
        self.url: str = url
        self.logger = logging.getLogger('#')

    def save(self):
        count = 1
        soup = get_soup(url=self.url)

        while soup is not None:
            table = soup.find('table', id='MasterGC_ContentBlockHolder_dgResultado')
            current_page = table.find('tr', class_='TablaPagineo').td.find_next('span').find_next('span')
            page_no = current_page.get_text()
            self.logger.info(f'### Processing Page: {page_no}')
            time.sleep(60)

            rows = [row for row in table.find_all('tr', class_=['FilaTablaDetalle', 'FilaTablaDetallef'])]

            self.logger.info(f'total row found: {len(rows)}')

            for row in rows:
                time.sleep(3)
                item = Item(
                    operation_number=row.find('label', title='NOG (Número de Operación Guatecompras)').get_text(
                        strip=True),
                    publication_date=row.find('label', title='Fecha de publicación').get_text(strip=True),
                    deadline_to_bid=row.find('label', title='Fecha límite para ofertar').get_text(strip=True),
                    can_participate=row.find('label', title='Puede participar cualquier proveedor').get_text(
                        strip=True),
                    contest_status=row.find('label', title='Estatus del concurso').get_text(strip=True),
                    publishing_entity=row.find('label', title='Entidad que publica').get_text(strip=True),
                    buying_unit=row.find('label', title='Unidad compradora que publica').get_text(strip=True),
                    contest_category=row.find('label', title='Categoría del concurso').get_text(strip=True),
                    contest_description=row.find('a', title='Descripción del concurso').get_text(strip=True),
                    contest_description_link=row.find('a', title='Descripción del concurso').get('href'),
                    modality=row.find_all('td')[-1].get_text(strip=True),
                )
                item.contest_description_link = urljoin(self.url, item.contest_description_link)

                if session.query(Item).filter_by(operation_number=item.operation_number).first() is None:
                    self.logger.info(f'writing {count} -> {item}')
                    item.page_data = get_page_data(url=item.contest_description_link).to_dict()
                    session.add(item)
                    session.commit()

                else:
                    self.logger.info(f'skipping {count} -> {item}')

                count += 1

            nxt_lnk = current_page.find_next('a')
            nxt_lnk = re.match(r"javascript:__doPostBack\('(.+)',''\)", nxt_lnk.get('href'))

            if nxt_lnk is None:
                self.logger.info('no next page found')
                break

            nxt_lnk = nxt_lnk.group(1)

            form = soup.find('form', id='aspnetForm')
            pd = {inp.get('name'): inp.get('value') for inp in form.find_all('input', value=True)}
            pd['MasterGC$ContentBlockHolder$ScriptManager1'] = f'MasterGC$ContentBlockHolder$UpdatePanel1|{nxt_lnk}'
            pd['__ASYNCPOST'] = True
            pd['__EVENTTARGET'] = {nxt_lnk}

            soup = get_soup(url=self.url, data=pd)


def main():
    urls: List[str] = [line.strip() for line in open(config.URLS_FILENAME).readlines() if line.strip()]

    for i, url in enumerate(urls):
        logger.info(f'[{i + 1}/{len(urls)}] {url}')
        app = App(url=url)
        app.save()


if __name__ == '__main__':
    main()
