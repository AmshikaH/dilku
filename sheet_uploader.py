import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import datetime
import csv
import logging
import sys
import traceback

# Date when the script is run 
date = datetime.datetime.now().strftime('%d%m%Y')

# Date and time when the script is run
dateAndTime = datetime.datetime.now().strftime('%d_%m_%Y,%H_%M_%S')

productMasterFileName = 'MasterProductDetail'

pathToSheetToBeUploaded = os.path.join('tagger', 'tagged', ('Tagged_' + productMasterFileName + '_' + date + '.csv'))

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

with open(pathToSheetToBeUploaded, newline='') as file:
    reader = csv.reader(file)
    csvValues = list(reader)

def batch_update_values(spreadsheet_id, range_name,
                        value_input_option, _values):
    
    creds, _ = google.auth.default()
    try:
        service = build('sheets', 'v4', credentials=creds)
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
        logger.info(f"{(result.get('totalUpdatedCells'))} cells updated.")
        return result
    except HttpError as error:
        logger.error(f"An error occurred: {error}")
        return error
    
if __name__ == '__main__':
    batch_update_values('14YwSVgEXUiaF65ej7NonLMOnym_InBsPWIj3r60N3l4',
                        'Sheet1', 'USER_ENTERED',
                        csvValues)

logger.info('Finished running script.')
