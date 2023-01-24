import requests
import csv
import json
import time

url = "https://ws2vapi.tse.org.gt:2087/api/tse/mesa"
myobj = {'PROCESO': 201902, 'NROMESA': 1}

f = open('result.csv', 'w', newline='')
writer = csv.writer(f)
logFile = open('log.txt', 'w')

for i in range(1, 21100):
    myobj['NROMESA'] = i
    print('scraping ' + str(i) + ' ....');
    try:
        response = requests.post(url, json = myobj);
        json_obj = json.loads(response.text)
        row = [
            int(json_obj['data'][0]['NROMESA']),
            int(json_obj['data'][0]['CNTPAPELETAS']),
            int(json_obj['data'][0]['V1']),
            int(json_obj['data'][0]['V2']),
            int(json_obj['data'][0]['VOTOSVALIDOSACTA']),
            int(json_obj['data'][0]['NULOS']),
            int(json_obj['data'][0]['BLANCOS']),
            int(json_obj['data'][0]['TOTALACTA']),
            int(json_obj['data'][0]['INVALIDOS']),
            int(json_obj['data'][0]['CNTIMPUGNA'])
        ]
        
        writer.writerow(row)
    except:
        print('failed to scrape ' + str(i))
        logFile.write('failed to scrape ' + str(i) + '\n')
    time.sleep(10)

logFile.close()
f.close()