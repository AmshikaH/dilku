import requests
import json
import csv
import os
import traceback
import sys
import datetime
import logging
import pandas as pd

# Date and time when the script is run
dateAndTime = datetime.datetime.now().strftime('%d_%m_%Y,%H_%M_%S')

# Directory containing the log files
logDirectoryName = 'logs'

# Format of the log file name
logFileNameFormat = '{}/script-{}.log'

# URL headers
headers = {'x-v': '3','x-min-v' : '900'}

# Date when the script is run
date = datetime.datetime.now().strftime("%d%m%Y")

# Directory containing the json files
mainDataDir = "jsonFiles"

# Path to directory to store the data dump for the date the script is run
dataDir = os.path.join(mainDataDir, date)

# File containing the bank API URLs
productMasterFileName = "products_master.txt"

# List of rows to be written to the csv file
rows = []

# Creating and setting up a logger
os.makedirs(logDirectoryName,exist_ok=True)
logger = logging.getLogger('script')
logger.setLevel(logging.DEBUG)
fileHandler = logging.FileHandler(logFileNameFormat.format(logDirectoryName, dateAndTime))
fileHandler.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(message)s')
fileHandler.setFormatter(formatter)
consoleHandler.setFormatter(formatter)
logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)

# Setting logger to log uncaught exceptions
def setExceptionHandler(exctype, value, tb):
    logger.exception(''.join(traceback.format_exception(exctype, value, tb)))
sys.excepthook = setExceptionHandler

logger.info('Script is running...')

# Exits program if master product file is not found
if not os.path.isfile(productMasterFileName):
        logger.error('Master product file ' + productMasterFileName + ' with bank API urls not found. Exiting program.')
        exit()
    
def dumpProducts():
    os.makedirs(dataDir, exist_ok=True)
    productFileNameFormat = "{}/{}-{}-obproducts.json"
    with open(productMasterFileName) as productMasterFile:
        csvReader = csv.reader(productMasterFile, delimiter=',')
        lineCount = 0
        for row in csvReader:
            if lineCount == 0:
                lineCount += 1
            else:
                if len(row) == 0:
                    return
                bankAPIUrl = row[1]
                fisName = row[0].replace(' ', '_')
                logger.info('Dumping product data for ' + row[0] + ' from ' + bankAPIUrl + '...')
                productFileName = productFileNameFormat.format(dataDir, fisName, date)
                response = requests.get(bankAPIUrl, headers=headers)
                jsonData = json.dumps(response.json())
                logger.info('Response from ' + bankAPIUrl + ': ' + str(response))
                with open(productFileName, 'w') as productFile:
                    productFile.write(jsonData)
                dumpProductDetails(fisName, date, bankAPIUrl)
                logger.info('Finished dumping product and product detail data for ' + row[0] + '.')
    logger.info('Finished dumping data.')

def dumpProductDetails(fisName, date, bankAPIUrl):
    inputProductFileNameFormat = "{}/{}-{}-obproducts.json"
    outputProductDetailFileNameFormat = "{}/{}-{}-{}_details.json"
    productDetailUrlFormat = "{}/{}"
    inputProductFileName = inputProductFileNameFormat.format(dataDir, fisName, date)    
    with open(inputProductFileName, 'r') as inputProductFile:
        customer = json.load(inputProductFile)
        productData = customer['data']['products']
        for x in range(len(productData)):
            # Only the data for products in the "RESIDENTIAL_MORTGAGES"
            # product category will be dumped
            if customer['data']['products'][x].get('productCategory') != 'RESIDENTIAL_MORTGAGES':
                return
            productId = productData[x]['productId']
            logger.info("Dumping product detail data for " + fisName + "'s product with id " + productId + "...")
            productDetailFileName = outputProductDetailFileNameFormat.format(dataDir, fisName, productId, date)
            productDetailUrl = productDetailUrlFormat.format(bankAPIUrl, productId)
            response = requests.get(productDetailUrl, headers=headers)
            jsonData = json.dumps(response.json())
            logger.info('Response from ' + productDetailUrl + ': ' + str(response))
            with open(productDetailFileName, 'w') as productDetailFile:
                        productDetailFile.write(jsonData)

def processJsonFile(directoryName, jsonFileName):
    filePath = os.path.join(directoryName, jsonFileName)
    if not os.path.isfile(filePath):
        return
    with open(filePath) as jsonFile:
        if jsonFileName == 'README.md':
            return
        try:
            data = json.load(jsonFile)
        except ValueError as e:
            logger.error('Skipping ' + jsonFileName + ' as it could not be loaded due to the following error: ')
            logger.error(traceback.format_exc())
            return

        # The brand and product id are mandatory fields;
        # if either of these are not found, the file being
        # processed will be skipped.
        brand = data['data'].get('brand')
        productId = data['data'].get('productId')
        if brand == None or productId == None:
            return

        # Lending rates are an optional field of data;
        # if no lending rates have been given, only the
        # mandatory fields will be written to the file.
        lendingRates = data['data'].get('lendingRates')
        if lendingRates == None or len(lendingRates) == 0:
            row = [brand, productId, None, '', '', '', '', '', '', '']
            rows.append(row)
            return

        # If lending rates data is found, the sub-fields
        # will be processed.
        for i in lendingRates:
            applicationFrequency = i.get('applicationFrequency')
            rate = i.get('rate')
            comparisonRate = i.get('comparisonRate')
            repaymentType = i.get('repaymentType')
            loanPurpose = i.get('loanPurpose')
            lvrMin = None
            lvrMax = None
            tiers = i.get('tiers')
            if tiers != None and len(tiers) != 0:
                for x in tiers:
                    if (x.get('name') == 'Loan to Value Ratio'):
                        lvrMin = x.get('minimumValue')
                        lvrMax = x.get('maximumValue')
            row = [brand, productId, lendingRates.index(i), lvrMin, lvrMax, applicationFrequency, rate, comparisonRate, repaymentType, loanPurpose]
            rows.append(row)

# Dumping product data
dumpProducts()

# Processing the files
directoryPath = os.path.join(mainDataDir, date)
listOfJsonFiles = os.listdir(directoryPath)
for x in listOfJsonFiles:
    logger.info('Processing file ' + x + '...')
    processJsonFile(directoryPath, x)

# Writing the processed data to the CSV file
fields = ['FISNAME', 'ProductID', 'LendingRates', 'LVR-Min', 'LVR-Max', 'applicationFrequency', 'Rate', 'Comparison Rate', 'repaymentType', 'loanPurpose']
csvFileName = 'MasterProductDetail_DDMMYY.csv'
with open(csvFileName, 'w', newline='') as csvFile:
    logger.info('Writing data to csv file ' + csvFileName + '...')
    csvWriter = csv.writer(csvFile) 
    csvWriter.writerow(fields)
    csvWriter.writerows(rows)

logger.info('Finished running script.')
logger.info('CSV file location: ' + str(os.getcwd()) + csvFileName)

