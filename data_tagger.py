import os
import yaml
import re
import logging
import datetime
import traceback
import sys
import ast
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

# Column names
rateHeader = 'rate'
repaymentTypeHeader = 'repaymentType'
loanPurposeHeader = 'loanPurpose'
descriptionHeader = 'description'
additionalInfoHeader = 'additionalInfo'
additionalValueHeader = 'additionalValue'
minimumValueHeader = 'minimumValue'
maximumValueHeader = 'maximumValue'
unitOfMeasureHeader = 'unitOfMeasure'
tierAdditionalInfoHeader = 'tierAdditionalInfo'

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
    return bool(re.search(r'\b' + re.escape(word.lower()) + r'\b', str(text).lower()))

def getEmptyIndices(df, header, removeTagIndices):
    emptyRepaymentTypeIndices = set(df[df[header].isnull()].index) - removeTagIndices
    return emptyRepaymentTypeIndices

def addNeedToFixTagBasedOnFieldData(df, removeTagIndices, patterns):
    for pattern in patterns:
        checkFields = pattern.get('checkFields')
        overwrite = pattern.get('overwrite')
        fillFields = pattern.get('fillFields')
        for fillField in fillFields:
            for field in checkFields:
                for fieldToFill in fillField.keys():
                    if overwrite.lower() == 'true':
                        indicesToCheck = set(df[fieldToFill].index) - removeTagIndices
                    else:
                        indicesToCheck = getEmptyIndices(df, fieldToFill, removeTagIndices)
                    options = fillField.get(fieldToFill)
                    exceptions = options.get('exceptions')
                    exceptionKeys = []
                    if exceptions != None:
                        exceptionKeys = exceptions.keys()
                    try:
                        for i in indicesToCheck:
                            filled = False
                            for option in options:
                                if option == 'exceptions':
                                    continue
                                if not filled:
                                    wordPatterns = options.get(option)
                                    for wordPattern in wordPatterns:
                                        if contains_word(df.loc[i, field], wordPattern):
                                            isException = False
                                            if wordPattern in exceptionKeys:
                                                exceptionValues = exceptions.get(wordPattern)
                                                for exceptionValue in exceptionValues:
                                                    if contains_word(df.loc[i, field], exceptionValue):
                                                        isException = True
                                            if not isException:
                                                df.loc[i, fieldToFill] = option
                                                filled = True
                                                df.loc[i, tagHeader] = needToFixTag
                                                break
                    except KeyError:
                        logger.warning('Cannot check field ' + field + ' for data patterns as the field is not found in file.')

def updateAdditionalValueUnits(df):
    additionalValueIndices = set(df[df[additionalValueHeader].notnull()].index)
    for i in additionalValueIndices:
        if re.search('^P[1-9][0-9]*Y$', df.loc[i, additionalValueHeader]) or re.search('^Fixed.[1-9][0-9]*.Year.*', df.loc[i, additionalValueHeader]):
            noOfYears = int(re.findall('[1-9][0-9]*', df.loc[i, additionalValueHeader])[0])
            noOfMonths = 12 * noOfYears
            additionalValue = 'P' + str(noOfMonths) + 'M'
            df.loc[i, additionalValueHeader] = additionalValue
        elif re.search('^.+Term.+[1-9][0-9]*.+Period.+', str(df.loc[i, additionalValueHeader]).replace(' ', '')):
            additionalValueDictionary = ast.literal_eval(df.loc[i, additionalValueHeader])
            period = additionalValueDictionary.get('Period')
            terms = int(additionalValueDictionary.get('Term'))
            if period == 'Month(s)':
                noOfMonths = terms
            else:
                noOfMonths = 12 * terms
            additionalValue = 'P' + str(noOfMonths) + 'M'
            df.loc[i, additionalValueHeader] = additionalValue

def getValue(df, field, index, pattern):
    text = re.findall(pattern, df.loc[index, field], re.IGNORECASE)[0]
    value = re.findall('[0-9][0-9]*', text)[0]
    return value

def patternExists(df, field, index, pattern):
    return re.search(pattern, df.loc[index, field], re.IGNORECASE)
    
def updateLVR(df, removeTagIndices, fieldsToCheck):
    fieldsToCheck = [additionalValueHeader, tierAdditionalInfoHeader, descriptionHeader, additionalInfoHeader]
    rowsToAdd = {}
    emptyMinimumValueIndices = getEmptyIndices(df, minimumValueHeader, removeTagIndices)
    emptyMaximumValueIndices = getEmptyIndices(df, maximumValueHeader, removeTagIndices)
    emptyLVRIndices = emptyMinimumValueIndices.union(emptyMaximumValueIndices)
    for field in fieldsToCheck:
        try:
            for i in emptyLVRIndices:
                tierValues = {}
                if not pd.isnull(df.loc[i, field]):
                    minimumValue = None
                    maximumValue = None
                    unitOfMeasure = None

                    if patternExists(df, field, i, '.*LVR.*up.to.*[0-9][0-9]*%.*'):
                        maximumValue = getValue(df, field, i, 'up.to.*[0-9][0-9]*.*')
                    elif patternExists(df, field, i, '.*to.[0-9][0-9]*%.*LVR'):
                        maximumValue = getValue(df, field, i, 'to.*[0-9][0-9]*.*')
                    elif patternExists(df, field, i, '.*<.*[0-9][0-9]*%.*'):
                        maximumValue = getValue(df, field, i, '<.*[0-9][0-9]*.*')
                    elif patternExists(df, field, i, '.*[0-9][0-9]*%.*>.*'):
                        maximumValue = getValue(df, field, i, '[0-9][0-9]*%.*>')
                    elif patternExists(df, field, i, '.*LVR.*[0-9][0-9]*%.or.less.*'):
                        maximumValue = getValue(df, field, i, 'LVR.*[0-9][0-9]*%.or.less')
                    elif patternExists(df, field, i, '.*LVR.*[0-9][0-9]*%.and.under.*'):
                        maximumValue = getValue(df, field, i, 'LVR.*[0-9][0-9]*%.and.under')
                    elif patternExists(df, field, i, 'Max.LVR.[0-9][0-9]*%.*'):
                        maximumValue = getValue(df, field, i, 'Max.LVR.[0-9][0-9]*%')
                    elif patternExists(df, field, i, 'under.*[0-9][0-9]*%.*LVR'):
                        maximumValue = getValue(df, field, i, 'under.*[0-9][0-9]*%.*LVR')
                    elif patternExists(df, field, i, '.*less.than.*[0-9][0-9]*%'):
                        maximumValue = getValue(df, field, i, 'less.than.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*not.exceeding.*[0-9][0-9]*%'):
                        maximumValue = getValue(df, field, i, 'not.exceeding.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*capped.at.total.lend.of.*[0-9][0-9]*%'):
                        maximumValue = getValue(df, field, i, 'capped.at.total.lend.of.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*Maximum.LVR.*[0-9][0-9]*%.*'):
                        if patternExists(df, field, i, '.*Maximum.LVR.for.owner.occupied.home.loans.is.[0-9][0-9]*%.and.[0-9][0-9]*%.for.investment.loans.*'):
                            text = re.findall('Maximum.LVR.for.owner.occupied.home.loans.is.[0-9][0-9]*%.and.[0-9][0-9]*%.for.investment.loans', df.loc[i, field], re.IGNORECASE)[0]
                            if df.loc[i, loanPurposeHeader] == 'OWNER_OCCUPIED':
                                maximumValue = re.findall('[0-9][0-9]*', text)[0]
                            elif df.loc[i, loanPurposeHeader] == 'INVESTMENT':
                                maximumValue = re.findall('[0-9][0-9]*', text)[1]
                        else:
                            maximumValue = getValue(df, field, i, 'Maximum.LVR.*[0-9][0-9]*%')
                        
                    if patternExists(df, field, i, '.*LVR.*>.*[0-9][0-9]*%.up.to.*'):
                        minimumValue = getValue(df, field, i, '.*LVR.*>.*[0-9][0-9]*%.*up.to')
                    elif patternExists(df, field, i, '.*>.*[0-9][0-9]*%.*'):
                        minimumValue = getValue(df, field, i, '>.*[0-9][0-9]*.*')
                    elif patternExists(df, field, i, '.*[0-9][0-9]*.?<.*'):
                        minimumValue = getValue(df, field, i, '[0-9][0-9]*.?<')
                    elif patternExists(df, field, i, '.*LVR.over.*[0-9][0-9]*%'):
                        minimumValue = getValue(df, field, i, 'LVR.over.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*more.than.*[0-9][0-9]*%'):
                        minimumValue = getValue(df, field, i, 'more.than.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*greater.than.*[0-9][0-9]*%'):
                        minimumValue = getValue(df, field, i, 'greater.than.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*Minimum.LVR.*[0-9][0-9]*%.*'):
                        minimumValue = getValue(df, field, i, 'Minimum.LVR.*[0-9][0-9]*%')

                    if patternExists(df, field, i, '.*between.[0-9][0-9]*%.*[0-9][0-9]*%.*'):
                        text = re.findall('between.[0-9][0-9]*%.*[0-9][0-9]*%', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.findall('[0-9][0-9]*', text)[0]
                        maximumValue = re.findall('[0-9][0-9]*', text)[1]
                    elif patternExists(df, field, i, '.*Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*.-.*[0-9][0-9]*'):
                        text = re.findall('Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*.-.*[0-9][0-9]*', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.findall('[0-9][0-9]*', text)[0]
                        maximumValue = re.findall('[0-9][0-9]*', text)[1]
                    elif patternExists(df, field, i, '.*Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*'):
                        text = re.findall('Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.findall('[0-9][0-9]*', text)[0]
                        
                    if minimumValue != None or maximumValue != None:
                        unitOfMeasure = 'PERCENT'
                    else:
                        continue
                    
                    tierValues[minimumValueHeader] = minimumValue
                    tierValues[maximumValueHeader] = maximumValue
                    tierValues[unitOfMeasureHeader] = unitOfMeasure
  
                    if i in rowsToAdd:
                        row = rowsToAdd.get(i)
                        minimumValue = minumumValue if row.get(minimumValueHeader) == None else row.get(minimumValueHeader)
                        maximumValue = maximumValue if row.get(maximumValueHeader) == None else row.get(maximumValueHeader)
                        unitOfMeasure = unitOfMeasure if row.get(unitOfMeasureHeader) == None else row.get(unitOfMeasureHeader)
                        continue
                    elif unitOfMeasure != None:
                        rowsToAdd[i] = tierValues
                        continue
        except KeyError:
            logger.warning('Cannot check field ' + field + ' for LVR data as the field is not found in file.')

    return rowsToAdd
    
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
    patterns = configs['Pattern']
    addNeedToFixTagBasedOnFieldData(df, removeTagIndices, patterns)

    # Add NeedtoCapture tags
    emptyRepaymentTypeIndices = getEmptyIndices(df, repaymentTypeHeader, removeTagIndices)
    emptyLoanPurposeIndices = getEmptyIndices(df, loanPurposeHeader, removeTagIndices)
    needToCaptureIndices = emptyRepaymentTypeIndices.union(emptyLoanPurposeIndices)
    for i in needToCaptureIndices:
        if df.loc[i, tagHeader] is None:
            df.loc[i, tagHeader] = needToCaptureTag
        else:
            df.loc[i, tagHeader] = (df.loc[i, tagHeader]) + ', ' + needToCaptureTag

    # Update additional value column units
    try:
        updateAdditionalValueUnits(df)
    except KeyError:
        logger.warning('Additional value field not found in file. Skipping changing of units from years to months.')

    rowsToAdd = updateLVR(df, removeTagIndices, [])
    dfLength = len(df.index)
    for rowIndex in rowsToAdd:
        write = False
        row = df.iloc[rowIndex]
        values = rowsToAdd.get(rowIndex)
        minValue = df.loc[i, minimumValueHeader]
        if pd.isnull(minValue) and values.get(minimumValueHeader) is not None:
            write = True
        maxValue = df.loc[i, maximumValueHeader]
        if pd.isnull(maxValue) and values.get(maximumValueHeader) is not None:
            write = True
        if write:
            df.loc[dfLength] = row
            df.loc[dfLength, minimumValueHeader] = values.get(minimumValueHeader)
            df.loc[dfLength, maximumValueHeader] = values.get(maximumValueHeader)
            df.loc[dfLength, unitOfMeasureHeader] = values.get(unitOfMeasureHeader)
            dfLength += 1

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
