import os
import yaml
import re
import logging
import datetime
import traceback
import sys
import pandas as pd

# Date and time when the script is run
dateAndTime = datetime.datetime.now().strftime('%d_%m_%Y,%H_%M_%S')

# Directory with files to be tagged
directoryWithFilesToTag = 'tagger'

# Directory with tagged files
directoryWithTaggedFiles =  'tagged'

# Config File Name
configFileName = 'config.yaml'

# Header for the column with the tags
tagHeader = 'tags'

# For rows with empty lending rates
removeTag = 'Remove'

# For rows with empty repaymentType or loanPurpose,
# but which are identifiable using other rows.
needToFixTag = 'NEED2FIX'

# For rows with empty repaymentType or loanPurpose,
# even after analysing the description.
needToCaptureTag = 'NeedtoCapture'

# Column name for rate
rateHeader = 'rate'

# Column name for repayment type
repaymentTypeHeader = 'repaymentType'

# Column name for loan purpose
loanPurposeHeader = 'loanPurpose'

# Repayment type options
repaymentTypeOptionOne = 'INTEREST_ONLY'
repaymentTypeOptionTwo = 'PRINCIPAL_AND_INTEREST'

# Loan Purpose options
loanPurposeOptionOne = 'INVESTMENT'
loanPurposeOptionTwo = 'OWNER_OCCUPIED'

os.makedirs(directoryWithFilesToTag, exist_ok=True)

# Creating and setting up a logger
logDirectoryName = os.path.join(directoryWithFilesToTag, 'logs')
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
    
# Setting logger to log uncaught exceptions
sys.excepthook = setExceptionHandler

logger.info('Script is running...')
with open(configFileName) as configFile:
    configs = yaml.load(configFile, Loader=yaml.FullLoader)
    
def contains_word(text, word):
    return bool(re.search(r'\b' + re.escape(word) + r'\b', text))

def tagAndFillUsingPattern(df, indicesToCheck, columnWithEmptyValue, columnValueOptionOne, columnValueOptionTwo):
    patternForOptionOne = configs['Pattern'][columnWithEmptyValue][columnValueOptionOne]
    patternForOptionTwo = configs['Pattern'][columnWithEmptyValue][columnValueOptionTwo]
    for i in indicesToCheck:
        filled = False
        for n in patternForOptionOne:
            if contains_word(df.loc[i, 'description'], n): 
                df.loc[i, columnWithEmptyValue] = columnValueOptionOne
                filled = True
                df.loc[i, tagHeader] = needToFixTag
                break
        if not filled:
            for n in patternForOptionTwo:
                if contains_word(df.loc[i, 'description'], n): 
                    df.loc[i, columnWithEmptyValue] = columnValueOptionTwo
                    df.loc[i, tagHeader] = needToFixTag
                    break

def getEmptyIndices(df, header, removeIndices):
    emptyRepaymentTypeIndices = set(df[df[header].isnull()].index) - removeIndices
    return emptyRepaymentTypeIndices
    
def tagFile(fileName):
    if fileName == 'README.md':
        return
    filePath = os.path.join(directoryWithFilesToTag, fileName)
    if not os.path.isfile(filePath):
        return
    logger.info('Tagging ' + file + '.')
    try:
        df = pd.read_csv(os.path.join(directoryWithFilesToTag, fileName), encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(os.path.join(directoryWithFilesToTag, fileName), encoding='cp1252')
        
    df[tagHeader] = None

    # Add remove tags
    removeTagIndices = set(df[df[rateHeader].isnull()].index)
    for i in removeTagIndices:
        df.loc[i, tagHeader] = removeTag

    # Check for patterns and add NEED2FIX tags
    emptyRepaymentTypeIndices = getEmptyIndices(df, repaymentTypeHeader, removeTagIndices)
    emptyLoanPurposeIndices = getEmptyIndices(df, loanPurposeHeader, removeTagIndices)
    tagAndFillUsingPattern(df, emptyRepaymentTypeIndices, repaymentTypeHeader, repaymentTypeOptionOne, repaymentTypeOptionTwo)
    tagAndFillUsingPattern(df, emptyLoanPurposeIndices, loanPurposeHeader, loanPurposeOptionOne, loanPurposeOptionTwo)
    
    # Add NeedtoCapture tags
    updatedEmptyRepaymentTypeIndices = getEmptyIndices(df, repaymentTypeHeader, removeTagIndices)
    updatedEmptyLoanPurposeIndices = getEmptyIndices(df, loanPurposeHeader, removeTagIndices)
    needToCaptureIndices = updatedEmptyRepaymentTypeIndices.union(updatedEmptyLoanPurposeIndices)
    for i in needToCaptureIndices:
        if df.loc[i, tagHeader] == None:
            df.loc[i, tagHeader] = needToCaptureTag
        else:
            df.loc[i, tagHeader] = (df.loc[i, tagHeader]) + ', ' + needToCaptureTag
    
    os.makedirs(os.path.join(directoryWithFilesToTag, directoryWithTaggedFiles), exist_ok=True)
    taggedFileName = 'Tagged_' + fileName
    df.to_csv(os.path.join(directoryWithFilesToTag, directoryWithTaggedFiles, taggedFileName), index=False)
    logger.info('The file ' + fileName + ' has been tagged and saved.')
    logger.info('CSV file path: ' + os.path.join(os.getcwd(), directoryWithFilesToTag, directoryWithTaggedFiles, taggedFileName))

filesToTag = os.listdir(directoryWithFilesToTag)
if not len(filesToTag) > 1:
    logger.warning('No files found to tag.')
    logger.warning('To tag files, add them to the "' + os.path.join(os.getcwd(), directoryWithFilesToTag) + '" directory and re-run the script.')

for file in filesToTag:
    tagFile(file)

logger.info('Finished running script.')
