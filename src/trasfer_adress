# -*- coding: UTF-8 -*-
import requests
import os
import json
import pandas as pd

def return_adress(lat,lng,shop_id):


    #lng='114'
    #lat='39.5'
    jingweidu=lat+','+lng

    r = requests.get(url='http://api.map.baidu.com/geocoder?/',params={'location': jingweidu,'output':'json','key':'vrvU8CB1E8AqaDxnGaYUFCoT02gylwzg'})
    result = r.json()
    print(result)
    province = result['result']['addressComponent']['province']
    city = result['result']['addressComponent']['city']
    district = result['result']['addressComponent']['district']
    print(province)
    print(city)
    print(district)


    file_path = os.path.join(os.getcwd(), '/mnt2/joe/store.txt')
    content = shop_id+'\t'+province+'\t'+city+'\t'+district+'\n'
    f = open(file_path, 'a')
    f.write(content)
    f.close()


if __name__ == '__main__':
    df = pd.read_csv('/mnt2/joe/example.csv',nrows=50,usecols =[0,1,2,3,4,5,27,28])

    lng=df['lon']
    lat=df['lat']
    shop_id=df['shop_id']
    print(id)
    for index, row in df.iterrows():
        print row["lon"], row["lat"]
        

        return_adress(str(row["lat"]),str(row["lon"]),str(row["shop_id"]))
