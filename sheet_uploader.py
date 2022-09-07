import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import datetime
import csv
import logging
import sys
import traceback
import yaml

# Date when the script is run 
date = datetime.datetime.now().strftime('%d%m%Y')

# Date and time when the script is run
dateAndTime = datetime.datetime.now().strftime('%d_%m_%Y,%H_%M_%S')

dateUploadedHeader = 'dateUploaded'

# Creating and setting up a logger
logDirectoryName = os.path.join('logs', 'sheet_uploader_logs')
logFileNameFormat = '{}/sheet-uploader-{}.log'
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
    
# Setting logger to log uncaught exceptions
sys.excepthook = setExceptionHandler

logger.info('Sheet uploader script is running...')

configFileName = 'config.yaml'
with open(configFileName) as configFile:
    configs = yaml.load(configFile, Loader=yaml.FullLoader)
productMasterFileName = 'MasterProductDetail'
defaultPathToSheetToBeUploaded = os.path.join('tagger', 'tagged', ('Tagged_' + productMasterFileName + '_' + date + '.csv'))
historicalData = configs['SheetUploader'].get('historicalData')

def readSheet(filePath):
    with open(filePath, newline='') as file:
        reader = csv.reader(file)
        fileValues = list(reader)
        return fileValues

csvValues = None
if historicalData != None and historicalData.lower() == 'true':
    pathToSheetToBeUploaded = configs['SheetUploader'].get('pathToSheetToBeUploaded')
    logger.info('Historical data upload is activated.')
    if pathToSheetToBeUploaded != None:
        logger.info('Uploading the sheet specified in config file...')
        try:
            csvValues = readSheet(pathToSheetToBeUploaded)
        except FileNotFoundError:
            logger.error('The sheet at ' + pathToSheetToBeUploaded + ' is not found. Uploading failed with the following error:')
            logger.error(traceback.format_exc())
    else:
        logger.error('No upload sheet path specified in config file.')
else:
    logger.info('Uploading the daily tagged sheet ' + defaultPathToSheetToBeUploaded + '...')
    try:
        csvValues = readSheet(defaultPathToSheetToBeUploaded)
    except FileNotFoundError:
        logger.error('The sheet at ' + defaultPathToSheetToBeUploaded + ' is not found. Uploading failed with the following error:')
        logger.error(traceback.format_exc())
if csvValues == None:
    logger.info('Finished running script.')
    exit()
    
uploadMode = configs['SheetUploader'].get('uploadMode')
def batch_update_values(spreadsheet_id, range_name,
                        value_input_option, _values):
    rowsToAppend = []
    creds, _ = google.auth.default()
    try:
        service = build('sheets', 'v4', credentials=creds)
        cellsUpdated = 0
        if uploadMode == 'overwrite':
            logger.info('Upload mode is set to overwrite. Overwriting all data in sheet...')
            result = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
            data = [
                {
                    'range': range_name,
                    'values': _values
                },
            ]
            body = {
                'valueInputOption': value_input_option,
                'data': data
            }
            result = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body).execute()
            cellsUpdated = result.get('totalUpdatedCells')
            range_name = nameOfSheetToBeUpdated + '!AA1:AA1'
            body = {
                'range': range_name,
                'majorDimension': 'COLUMNS',
                'values': [[dateUploadedHeader]]
            }
            result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id, body=body, range=range_name, valueInputOption=value_input_option).execute()
            cellsUpdated = cellsUpdated + result.get('updates').get('updatedCells')
            range_name = nameOfSheetToBeUpdated + '!AA2:AA' + str(len(_values))
            listDateUploaded = [date]
            dateList = []
            for n in range(1, len(_values)):
                dateList.append(listDateUploaded)
            data = [
                {
                    'range': range_name,
                    'values': dateList
                }
            ]
            body = {
                'valueInputOption': value_input_option,
                'data': data
            }
            result = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body).execute()
            cellsUpdated = cellsUpdated + result.get('totalUpdatedCells')
            logger.info(f"{(cellsUpdated)} cell(s) updated.")
        else:
            if uploadMode == 'append':
                logger.info('Upload mode is set to append. Appending new data to sheet...')
            else:
                logger.info('Upload mode is not set to a valid value. Using default; appending new data to sheet...')
            updateRange = nameOfSheetToBeUpdated + '!A:Z'
            result = service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id, ranges=updateRange).execute()
            ranges = result.get('valueRanges', [])
            sheetRows = ranges[0].get('values')
            if sheetRows != None:
                for i in _values:
                    if i[25] == '':
                        del i[25]
                    if i not in sheetRows:
                        rowsToAppend.append(i)
            else:
                rowsToAppend = _values
            body = {
                'values': rowsToAppend
            }
            result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_input_option, body=body).execute()
            cellsUpdated = (result.get('updates').get('updatedCells') if result.get('updates').get('updatedCells') != None else 0)
            dateHeaderRange = nameOfSheetToBeUpdated + '!AA1:AA1'
            firstRowRange = nameOfSheetToBeUpdated + '!1:1'
            result = service.spreadsheets().values().batchGet(
                spreadsheetId=spreadsheet_id, ranges=firstRowRange).execute()
            firstRowValues = result.get('valueRanges', [])[0].get('values')[0]
            if len(firstRowValues) > 26:
                if firstRowValues[26] != dateUploadedHeader:
                    body = {
                        'values': [[dateUploadedHeader]]
                    }
                    result = service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id, range=dateHeaderRange,
                        valueInputOption=value_input_option, body=body).execute()
                    cellsUpdated = cellsUpdated + result.get('updatedCells')
            else:
                body = {
                    'range': dateHeaderRange,
                    'majorDimension': 'COLUMNS',
                    'values': [[dateUploadedHeader]]
                }
                result = service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, body=body, range=dateHeaderRange,
                valueInputOption=value_input_option).execute()
                cellsUpdated = cellsUpdated + result.get('updates').get('updatedCells')
            existingRowLength = len(sheetRows) if sheetRows != None else 1
            dateCellsLength = len(rowsToAppend) if sheetRows != None else (len(rowsToAppend) - 1)
            range_name = nameOfSheetToBeUpdated + '!AA' + str(existingRowLength + 1) + ':AA' + str(existingRowLength + dateCellsLength)
            listDateUploaded = [date]
            dateList = []
            for n in range(1, dateCellsLength + 1):
                dateList.append(listDateUploaded)
            data = [
                {
                    'range': range_name,
                    'values': dateList
                }
            ]
            body = {
                'valueInputOption': value_input_option,
                'data': data
            }
            result = service.spreadsheets().values().batchUpdate(
                spreadsheetId=spreadsheet_id, body=body).execute()
            cellsUpdated = (0 if cellsUpdated == None else cellsUpdated) + (0 if result.get('totalUpdatedCells') == None else result.get('totalUpdatedCells'))
            logger.info(f"{(cellsUpdated)} cell(s) appended.")
        return result
    except HttpError as error:
        logger.error(f"An error occurred: {error}")
        return error
    
if __name__ == '__main__':
    spreadsheetId = configs['SheetUploader'].get('spreadsheetId')
    nameOfSheetToBeUpdated = configs['SheetUploader'].get('nameOfSheetToBeUpdated')
    batch_update_values(spreadsheetId,
                        nameOfSheetToBeUpdated, 'RAW',
                        csvValues)

logger.info('Finished running script.')
