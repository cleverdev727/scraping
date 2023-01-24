import requests
import re
from bs4 import BeautifulSoup 

url = "https://www.guatecompras.gt/concursos/consultaConcurso.aspx?nog=16314158&o=4"
url = "https://www.guatecompras.gt/proveedores/DetallePubliOfertaElectronicatp.aspx?Pro=4708660&nog=14172267&o=5"
# url = "https://www.guatecompras.gt/proveedores/DetallePubliOfertaElectronicatp.aspx?Pro=138&nog=14172267&o=5"
response = requests.get(url);
content = BeautifulSoup(response.content, "html.parser")

jsonData = [];

table = content.find(id="MasterGC_ContentBlockHolder_RadTipoProd")
thead = table.find('tr', class_="HeaderTablaDetalle")
ths = thead.find_all('td')
thAry = []
for th in ths:
    thAry.append(th.text)

trs = table.find_all('tr', class_="FilaTablaDetalle")
for tr in trs:
    tds = tr.find_all('td')
    ind = 0
    tdObj = {}
    for td in tds:
        tdObj[thAry[ind]] = td.text.rsplit('\n')[1]
        ind += 1
    jsonData.append(tdObj)
print(jsonData)

# f = open("result.txt", "w")
# f.write(jsonData);
# f.close()