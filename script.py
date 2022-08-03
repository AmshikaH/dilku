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

# Date when the script is run
date = datetime.datetime.now().strftime('%d%m%Y')

# URL headers
headers = {'x-v': '3','x-min-v' : '900'}

# Directory containing the json files
mainDataDir = 'jsonFiles'

# Path to directory to store the data dump for the date the script is run
dataDir = os.path.join(mainDataDir, date)

# File containing the bank API URLs
productMasterFileName = 'products_master.txt'

# List of rows to be written to the csv file
rows = []

# Creating and setting up a logger
logDirectoryName = 'logs'
logFileNameFormat = '{}/script-{}.log'
os.makedirs(logDirectoryName, exist_ok=True)
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

def setExceptionHandler(exctype, value, tb):
    logger.exception(''.join(traceback.format_exception(exctype, value, tb)))
    
def dumpProducts():
    fileDirectory = os.path.join(dataDir, 'productFiles')
    os.makedirs(fileDirectory, exist_ok=True)
    productFilePathFormat = '{}/{}-{}-obproducts.json'
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
                productFilePath = productFilePathFormat.format(fileDirectory, fisName, date)
                try:
                    response = requests.get(bankAPIUrl, headers=headers)
                except requests.exceptions.SSLError:
                    print('Caught')
                    logger.error('The SSL certificate could not be verified due to the following error:')
                    logger.error(traceback.format_exc())
                    logger.info('Retrying with SSL verification disabled.')
                    response = requests.get(bankAPIUrl, headers=headers, verify=False)
                logger.info('Response: ' + str(response))
                if response.status_code != 200:
                    logger.error('Failed to get product data for ' + fisName + ' due to: ' + response.reason)
                    continue
                jsonData = json.dumps(response.json())
                with open(productFilePath, 'w') as productFile:
                    productFile.write(jsonData)
                dumpProductDetails(fisName, bankAPIUrl, productFilePath)
                logger.info('Finished dumping product and product detail data for ' + row[0] + '.')
    logger.info('Finished dumping data.')

def dumpProductDetails(fisName, bankAPIUrl, inputProductFilePath):
    fileDirectory = os.path.join(dataDir, 'productDetailFiles')
    os.makedirs(fileDirectory, exist_ok=True)
    outputProductDetailFilePathFormat = '{}/{}-{}-{}_details.json'
    productDetailUrlFormat = '{}/{}' 
    with open(inputProductFilePath, 'r') as inputProductFile:
        customer = json.load(inputProductFile)
        if customer.get('data') == None:
            logger.info('Skipping ' + fisName + ' as no product data is found.')
            return
        if customer['data'].get('products') == None:
            logger.info('Skipping ' + fisName + ' as no product data is found.')
            return
        productData = customer['data']['products']
        for x in range(len(productData)):
            # Only the data for products in the "RESIDENTIAL_MORTGAGES"
            # product category will be dumped.
            if customer['data']['products'][x].get('productCategory') != 'RESIDENTIAL_MORTGAGES':
                return
            productId = productData[x]['productId']
            logger.info("Dumping product detail data for " + fisName + "'s product with id " + productId + "...")
            productDetailFilePath = outputProductDetailFilePathFormat.format(fileDirectory, fisName, productId, date)
            productDetailUrl = productDetailUrlFormat.format(bankAPIUrl, productId)
            try:
                response = requests.get(productDetailUrl, headers=headers)
            except SSLCertVerificationError:
                logger.logger('The SSL certificate could not be verified due to the following error:')
                logger.error(traceback.format_exc())
                logger.info('Retrying with SSL verification disabled.')
                response = requests.get(productDetailUrl, headers=headers, verify=False)
            logger.info('Response: ' + str(response))
            if response.status_code != 200:
                logger.error("Failed to get product detail data for " + fisName + "'s product with id " + productId
                             + " due to: " + response.reason)
                continue
            jsonData = json.dumps(response.json())
            with open(productDetailFilePath, 'w') as productDetailFile:
                productDetailFile.write(jsonData)

def processJsonFile(directoryName, jsonFileName):
    filePath = os.path.join(directoryName, jsonFileName)
    if not os.path.isfile(filePath):
        return
    logger.info('Processing file ' + jsonFileName + '...')
    with open(filePath) as jsonFile:
        if jsonFileName == 'README.md':
            return
        try:
            data = json.load(jsonFile)
        except ValueError as e:
            logger.error('Skipping ' + jsonFileName + ' as it could not be loaded due to the following error: ')
            logger.error(traceback.format_exc())
            return

        # The following checks for the mandatory fields for
        # each product; if any of these are not found, the
        # file being processed will be skipped.
        if data.get('data') == None:
            logger.error('Skipping ' + jsonFileName + ' as one or more mandatory fields are not found.')
            return
        productId = data['data'].get('productId')
        lastUpdated = data['data'].get('lastUpdated')
        productCategory = data['data'].get('productCategory')
        name = data['data'].get('name')
        brand = data['data'].get('brand')
        description = data['data'].get('description')
        if productId == None or lastUpdated == None or productCategory == None or name == None or brand == None or description == None:
            logger.error('Skipping ' + jsonFileName + ' as one or more mandatory fields are not found.')
            return

        # The following are optional fields;
        # if any of these are not found, the file would
        # continue to be processed and the fields will be left empty.
        effectiveFrom = data['data'].get('effectiveFrom')
        brandName = data['data'].get('brandName')
        applicationUri = data['data'].get('applicationUri')
        
        # Lending rates are an optional field of data;
        # if no lending rates have been given, only the
        # mandatory fields will be written to the file.
        lendingRates = data['data'].get('lendingRates')
        if lendingRates == None or len(lendingRates) == 0:
            row = [productId, effectiveFrom, lastUpdated, productCategory, name, brand, brandName, applicationUri,
                   '', '', '', '', '', '', '', '', '', '', description]
            rows.append(row)
            return

        # If lending rates data is found, the sub-fields
        # will be processed.
        for i in lendingRates:
            lendingRateType = i.get('lendingRateType')
            rate = i.get('rate')

            # The following checks for the mandatory fields for each
            # lending rate; if either of these are not found, the
            # relevant lending rate will be skipped.
            if lendingRateType == None or rate == None:
                logger.warn('Skipping a lending rate in file ' + jsonFileName + ' since one or more mandatory '
                            + 'fields regarding the lending rate are not found.')
                        
            comparisonRate = i.get('comparisonRate')
            calculationFrequency = i.get('calculationFrequency')
            applicationFrequency = i.get('applicationFrequency')
            repaymentType = i.get('repaymentType')
            loanPurpose = i.get('loanPurpose')
            minimumValue = None # mandatory
            maximumValue = None
            unitOfMeasure = None # mandatory
            tiers = i.get('tiers')
            if tiers != None and len(tiers) != 0:
                for x in tiers:
                    minimumValue = x.get('minimumValue')
                    maximumValue = x.get('maximumValue')
                    unitOfMeasure = x.get('unitOfMeasure')

                    # The following checks for the mandatory fields;
                    # if either of these are not found, the relevant
                    # tier will be skipped.
                    if minimumValue == None or unitOfMeasure == None:
                        logger.warn('Skipping tier in file ' + jsonFileName + ' since one or more mandatory '
                                    + 'fields regarding the tier are not found.')
                        
                    row = [productId, effectiveFrom, lastUpdated, productCategory, name, brand, brandName,
                           applicationUri, lendingRateType, rate, comparisonRate, calculationFrequency, applicationFrequency,
                           repaymentType, loanPurpose, minimumValue, maximumValue, unitOfMeasure, description]
                    rows.append(row)

# Setting logger to log uncaught exceptions
sys.excepthook = setExceptionHandler

logger.info('Script is running...')

# Exiting the program if the master product file is not found
if not os.path.isfile(productMasterFileName):
    logger.error('Master product file ' + productMasterFileName + ' with bank API urls not found. Exiting program.')
    exit()
        
# Dumping product data
dumpProducts()

# Processing the files
directoryPath = os.path.join(mainDataDir, date, 'productDetailFiles')
listOfJsonFiles = os.listdir(directoryPath)
for x in listOfJsonFiles:
    processJsonFile(directoryPath, x)
             
# Writing the processed data to the CSV file
fields = ['productId', 'effectiveFrom', 'lastUpdated', 'productCategory', 'name', 'brand', 'brandName',
          'applicationUri', 'lendingRateType', 'rate', 'comparisonRate', 'calculationFrequency', 'applicationFrequency',
          'repaymentType', 'loanPurpose', 'minimumValue', 'maximumValue', 'unitOfMeasure', 'description']
csvFileDirectory = 'csvOutputFiles'
os.makedirs(csvFileDirectory, exist_ok=True)
csvFileNameFormat = 'MasterProductDetail_{}.csv'
csvFileName = csvFileNameFormat.format(date)
csvFilePath = os.path.join(csvFileDirectory, csvFileName)
with open(csvFilePath, 'w', newline='') as csvFile:
    logger.info('Writing data to csv file ' + csvFileName + '...')
    csvWriter = csv.writer(csvFile) 
    csvWriter.writerow(fields)
    csvWriter.writerows(rows)

logger.info('CSV file location: ' + os.path.join(str(os.getcwd()), csvFileName))
logger.info('Finished running script.')

