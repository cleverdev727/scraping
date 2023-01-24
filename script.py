import requests
import re
from bs4 import BeautifulSoup
import xlwt
from xlwt import Workbook

wb = Workbook()
sheet1 = wb.add_sheet('Sheet 1')
url = "https://www.fundamentus.com.br/resultado.php"
agent = {"User-Agent":"Mozilla/5.0"}
response = requests.get(url, headers=agent)
content = BeautifulSoup(response.text, "html.parser")

table = content.find('table', id="resultado")
thead = table.find('thead')
ths = thead.find('tr').find_all('th')
thAry = []
colNum = -1
for th in ths:
    thAry.append(th.text)
    colNum += 1
    sheet1.write(0, colNum, th.text)

print(thAry)

rowNum = 0
tbody = table.find('tbody')
trs = tbody.find_all('tr')
for tr in trs:
    rowNum += 1
    tds = tr.find_all('td')
    colNum = -1
    for td in tds:
        colNum += 1
        sheet1.write(rowNum, colNum, td.text)
        # tdObj[thAry[ind]] = td.text.rsplit('\n')[1]

wb.save('data.xls')