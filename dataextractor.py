#!/usr/bin/python3
import requests
import json
import csv
import os
import datetime
import pandas as pd

filename = "products_master.txt"
headers = {'x-v': '3','x-min-v' : '900'}
date = datetime.datetime.now().strftime("%d%m%Y")
datadir = "/home/dilku/source/OBanking/data"
prodmasterfile = "/home/dilku/source/OBanking/products_master.txt"

def remove(string):
    return string.replace(" ", "_")

def dump_products():
    os.makedirs(datadir,exist_ok=True)
    filename = "{}/{}-{}-obproducts.json"
    #with open('products_master.txt') as csv_file:
    with open(prodmasterfile) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
            else:
                url = row[1]
                fisname = remove(row[0])
                x = filename.format(datadir,fisname,date)
                response = requests.get(url, headers=headers)
                op = json.dumps(response.json())
                print(response, fisname)
                f = open(x, "w")
                f.write(op)
                f.close()
                dump_product_details(fisname,date)
                

def dump_productids2_csv(fisname,date):
    #this is not required, all the prodouctIds are directly read from the JSON file 
    filename_input = "{}/{}-{}-obproducts.json"
    filename_output = "{}/{}-{}-obproducts-detail-master.csv"
    x = filename_input.format(datadir,fisname,date)
    y = filename_output.format(datadir,fisname,date)
    with open(x) as file:
        data = json.load(file)
        #print (type(data['data']['products']))
        df = pd.DataFrame(data['data']['products'])
        df.to_csv(y, index=False)
        print (data["data"]["products"][1]["productId"])
    file.close()

def dump_product_details(fisname,date):
    filename_output = "{}/{}-{}-{}_details.json"
    filename_input = "{}/{}-{}-obproducts.json"
    prod_detail_url = "{}/{}"
    ff = filename_input.format(datadir,fisname,date)    
    f = open(ff,"r")
    customer = json.load(f)
    w = customer['data']['products']
    burl = findtheURL(prodmasterfile,fisname)
    for x in range(len(w)):
        productId = w[x]["productId"]
        file = filename_output.format(datadir,fisname,productId,date)
        url = prod_detail_url.format(burl,productId)
        response = requests.get(url, headers=headers)
        op = json.dumps(response.json())
        fo = open(file, "w")
        fo.write(op)
        fo.close()
        print (response)
    f.close()


def findtheURL(filename,fisname): 
    with open(filename, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            if fisname == row[0]:
            #print(row[1])
                return row[1]

dump_products()
#dump_productids2_csv(fisname,date)
#dump_product_details(fisname,date)
exit()
