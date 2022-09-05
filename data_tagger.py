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

# Tags
goodTag = 'Good'

alt = 'Alt'

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
minimumValueAltHeader = minimumValueHeader + alt
maximumValueAltHeader = maximumValueHeader + alt
unitOfMeasureAltHeader = unitOfMeasureHeader + alt
tierAdditionalInfoHeader = 'tierAdditionalInfo'
lendingRateTypeHeader = 'lendingRateType'
productIdHeader = 'productId'
nameHeader = 'name'

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
    contains = bool(re.search(r'\b' + re.escape(word.lower()) + r'\b', str(text).lower()))
    if contains:
        return contains
    if '_' in str(text) or '-' in str(text):
        text = str(text).replace('_', ' ')
        text = str(text).replace('-', ' ')
        contains = bool(re.search(r'\b' + re.escape(word.lower()) + r'\b', str(text).lower()))
    if contains:
        return contains
    text = re.sub(r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))', r'\1 ', str(text))
    contains = bool(re.search(r'\b' + re.escape(word.lower()) + r'\b', str(text).lower()))
    return contains

def getEmptyIndices(df, header):
    return set(df[df[header].isnull()].index)

def addNeedToFixTagBasedOnFieldData(df, patterns):
    for pattern in patterns:
        checkFields = pattern.get('checkFields')
        overwrite = pattern.get('overwrite')
        fillFields = pattern.get('fillFields')
        for fillField in fillFields:
            for field in checkFields:
                for fieldToFill in fillField.keys():
                    logger.info('Checking ' + field + ' for ' + fieldToFill + ' data...')
                    if overwrite.lower() == 'true':
                        logger.info('Overwrite is set to true. Existing data in ' + fieldToFill + ' will be overwritten'
                                    + ' if relevant information is found in ' + field + '.')
                        indicesToCheck = set(df[fieldToFill].index)
                    else:
                        logger.info('Overwrite is set to false. Any relevant data found in ' + field + ' will be written'
                                    + ' to ' + fieldToFill + ' only if ' + fieldToFill + ' is empty.')
                        indicesToCheck = getEmptyIndices(df, fieldToFill)
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
                                                break
                    except KeyError:
                        logger.warning('Cannot check field ' + field + ' for data patterns as the field is not found in file.')

def getValue(df, field, index, pattern):
    text = re.findall(pattern, df.loc[index, field], re.IGNORECASE)[0]
    value = re.findall('[0-9][0-9]*', text)[0]
    return value

def patternExists(df, field, index, pattern):
    return re.search(pattern, df.loc[index, field], re.IGNORECASE)

def updateAdditionalValue(df):
    logger.info('Checking productId for additional value data for fixed lending rate type...')
    additionalValueEmptyIndices = getEmptyIndices(df, additionalValueHeader)
    for i in additionalValueEmptyIndices:
        if df.loc[i, lendingRateTypeHeader] == 'FIXED' or df.loc[i, lendingRateTypeHeader] == 'VARIABLE':
            if patternExists(df, nameHeader, i, '[0-9][0-9]*.year|[0-9][0-9]*.*YR|[0-9][0-9]*Y|[0-9][0-9]*.*mths|[0-9][0-9]*.*month'):
                additionalValue = int(getValue(df, nameHeader, i, '[0-9][0-9]*.year|[0-9][0-9]*.*YR|[0-9][0-9]*Y|[0-9][0-9]*.*mths|[0-9][0-9]*.*month')) * 12
                df.loc[i, additionalValueHeader] = 'P' + str(additionalValue) + 'M'
    logger.info('Updating additionalValue column units...')
    additionalValueIndices = set(df[df[additionalValueHeader].notnull()].index)
    for i in additionalValueIndices:
        if re.search('^P[1-9][0-9]*Y$', df.loc[i, additionalValueHeader]) or re.search('^Fixed.[1-9][0-9]*.Year.*', df.loc[i, additionalValueHeader]):
            noOfYears = int(re.findall('[1-9][0-9]*', df.loc[i, additionalValueHeader])[0])
            noOfMonths = 12 * noOfYears
            months = 'months' if noOfMonths > 1 else 'month'
            additionalValue = str(noOfMonths) + ' ' + months
            df.loc[i, additionalValueHeader] = additionalValue
        if re.search('^P[1-9][0-9]*M$', df.loc[i, additionalValueHeader]):
            noOfMonths = int(re.findall('[1-9][0-9]*', df.loc[i, additionalValueHeader])[0])
            months = 'months' if noOfMonths > 1 else 'month'
            additionalValue = str(noOfMonths) + ' ' + months
            df.loc[i, additionalValueHeader] = additionalValue
        elif re.search('^.+Term.+[1-9][0-9]*.+Period.+', str(df.loc[i, additionalValueHeader]).replace(' ', '')):
            additionalValueDictionary = ast.literal_eval(df.loc[i, additionalValueHeader])
            period = additionalValueDictionary.get('Period')
            terms = int(additionalValueDictionary.get('Term'))
            if period == 'Month(s)':
                noOfMonths = terms
            else:
                noOfMonths = 12 * terms
            months = 'months' if noOfMonths > 1 else 'month'
            additionalValue = str(noOfMonths) + ' ' + months
            df.loc[i, additionalValueHeader] = additionalValue
                
def updateLVR(df, fieldsToCheck):
    rowsToAdd = {}
    for field in fieldsToCheck:
        logger.info('Checking ' + field + ' for LVR values...')
        try:
            emptyLVRIndices = getEmptyIndices(df, minimumValueHeader).union(getEmptyIndices(df, maximumValueHeader))
            for i in emptyLVRIndices:
                tierValues = {minimumValueHeader: None, maximumValueHeader: None, unitOfMeasureHeader: None}
                if not pd.isnull(df.loc[i, field]):
                    minimumValue = None
                    maximumValue = None
                    unitOfMeasure = None
                    if patternExists(df, field, i, '.*LVR.*up.to.*[0-9][0-9]*%.*|to.[0-9][0-9]*%.*LVR|LVR.to.[0-9][0-9]*%|LVR.?<[0-9][0-9]*|.*<.*[0-9][0-9]*.*|.*[0-9][0-9]*%.*>.*|.*LVR.*[0-9][0-9]*%.or.less.*|.*LVR.*[0-9][0-9]*%.and.under.*|Max.LVR.[0-9][0-9]*%.*|under.*[0-9][0-9]*%.*LVR|LVR.under.*[0-9][0-9]*%|LVR.is.under.*[0-9][0-9]*%|LVR.is.below.*[0-9][0-9]*%|LVR.is.lower.*or.equal.to.[0-9][0-9]*%|LVR.LE.[0-9][0-9]*%|.*less.than.*[0-9][0-9]*%|.*not.exceeding.*[0-9][0-9]*%|.*capped.at.total.lend.of.*[0-9][0-9]*%'):
                        maximumValue = getValue(df, field, i, 'up.to.*[0-9][0-9]*.*|to.[0-9][0-9]*.*|LVR.to.[0-9][0-9]*%|LVR.?<[0-9][0-9]*|<.*[0-9][0-9]*.*|[0-9][0-9]*%.*>|LVR.*[0-9][0-9]*%.or.less|LVR.*[0-9][0-9]*%.and.under|Max.LVR.[0-9][0-9]*%|under.*[0-9][0-9]*%.*LVR|LVR.under.*[0-9][0-9]*%|LVR.is.under.*[0-9][0-9]*%|LVR.is.below.*[0-9][0-9]*%|LVR.is.lower.*.or.equal.to.[0-9][0-9]*%|LVR.LE.[0-9][0-9]*%|less.than.*[0-9][0-9]*%|not.exceeding.*[0-9][0-9]*%|capped.at.total.lend.of.*[0-9][0-9]*%')
                    elif patternExists(df, field, i, '.*Maximum.LVR.*[0-9][0-9]*%.*'):
                        if patternExists(df, field, i, '.*Maximum.LVR.for.owner.occupied.home.loans.is.[0-9][0-9]*%.and.[0-9][0-9]*%.for.investment.loans.*'):
                            text = re.findall('Maximum.LVR.for.owner.occupied.home.loans.is.[0-9][0-9]*%.and.[0-9][0-9]*%.for.investment.loans', df.loc[i, field], re.IGNORECASE)[0]
                            if df.loc[i, loanPurposeHeader] == 'OWNER_OCCUPIED':
                                maximumValue = re.findall('[0-9][0-9]*', text)[0]
                            elif df.loc[i, loanPurposeHeader] == 'INVESTMENT':
                                maximumValue = re.findall('[0-9][0-9]*', text)[1]
                        else:
                            maximumValue = getValue(df, field, i, 'Maximum.LVR.*[0-9][0-9]*%')
                    if patternExists(df, field, i, 'Starting.from.[0-9][0-9]*%.LVR|LVR.?>[0-9][0-9]*|.*LVR.*>.*[0-9][0-9]*%.up.to.*|.*>.*[0-9][0-9]*%.*|.*[0-9][0-9]*.?<.*|.*LVR.over.*[0-9][0-9]*%|.*more.than.*[0-9][0-9]*%|.*greater.than.*[0-9][0-9]*%|.*Minimum.LVR.*[0-9][0-9]*%.*|LVR.is.over.*[0-9][0-9]*%'):
                        minimumValue = getValue(df, field, i, 'Starting.from.[0-9][0-9]*%.LVR|LVR.?>[0-9][0-9]*|.*LVR.*>.*[0-9][0-9]*%.*up.to|>.*[0-9][0-9]*.*|[0-9][0-9]*.?<|LVR.over.*[0-9][0-9]*%|more.than.*[0-9][0-9]*%|greater.than.*[0-9][0-9]*%|Minimum.LVR.*[0-9][0-9]*%|LVR.is.over.*[0-9][0-9]*%')
                    if patternExists(df, field, i, 'between.[0-9][0-9]*.*[0-9][0-9]*%.*|.*Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*.-.*[0-9][0-9]*|LVR.[0-9][0-9]*%.to.[0-9][0-9]%|LVR.*[0-9][0-9]*.?-.?[0-9][0-9]*%|[0-9][0-9]*%.?-.?[0-9][0-9]*%.LVR'):
                        text = re.findall('between.[0-9][0-9]*.*[0-9][0-9]*%|Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*.-.*[0-9][0-9]*|LVR.[0-9][0-9]*%.to.[0-9][0-9]*%|LVR.*[0-9][0-9]*.?-.?[0-9][0-9]*%|[0-9][0-9]*%.?-.?[0-9][0-9]*%.LVR', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.findall('[0-9][0-9]*', text)[0]
                        maximumValue = re.findall('[0-9][0-9]*', text)[1]
                    elif patternExists(df, field, i, '.*Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%.*'):
                        text = re.findall('Rate.for.LVR.[0-9][0-9]*%.*-.*[0-9][0-9]*%', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.findall('[0-9][0-9]*', text)[0]
                    if minimumValue != None or maximumValue != None:
                        tierValues[unitOfMeasureHeader] = 'PERCENT'
                    else:
                        continue
                    if i in rowsToAdd:
                        row = rowsToAdd.get(i)
                        tierValues[minimumValueHeader] = minimumValue if row.get(minimumValueHeader) == None else row.get(minimumValueHeader)
                        tierValues[maximumValueHeader] = maximumValue if row.get(maximumValueHeader) == None else row.get(maximumValueHeader)
                    else:
                        tierValues[minimumValueHeader] = minimumValue
                        tierValues[maximumValueHeader] = maximumValue
                        rowsToAdd[i] = tierValues
        except KeyError:
            logger.warning('Cannot check field ' + field + ' for LVR data as the field is not found in file.')
    return rowsToAdd

def updateLVRAlt(df, fieldsToCheck):
    rowsToAdd = {}
    emptyLVRIndices = getEmptyIndices(df, minimumValueAltHeader).union(getEmptyIndices(df, maximumValueAltHeader))
    for field in fieldsToCheck:
        logger.info('Checking ' + field + ' for LVR values in an alternate unit of measure...')
        try:
            for i in emptyLVRIndices:
                tierValues = {}
                if not pd.isnull(df.loc[i, field]):
                    minimumValue = None
                    maximumValue = None
                    unitOfMeasure = None
                    if patternExists(df, field, i, 'above.*\$[0-9][0-9,]*k|over.*\$[0-9][0-9,]*k|greater.than.*\$[0-9][0-9,]*k|\$[0-9][0-9,]*k.or.more|\$[0-9][0-9,]*k.plus|>.*\$[0-9][0-9,]*k'):
                        minimumValue = re.sub('[kK]', '000', re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*k', re.findall('above.*\$[0-9][0-9,]*k|over.*\$[0-9][0-9,]*k|greater.than.*\$[0-9][0-9,]*k|\$[0-9][0-9,]*k.or.more|\$[0-9][0-9,]*k.plus|>.*\$[0-9][0-9,]*k', df.loc[i, field], re.IGNORECASE)[0], re.IGNORECASE)[0]))
                    elif patternExists(df, field, i, 'minimum.*\$[0-9][0-9,]*|above.*\$[0-9][0-9,]*|over.*\$[0-9][0-9,]*|greater.than.*\$[0-9][0-9,]*|\$[0-9][0-9,]*.or.more|\$[0-9][0-9,]*.and.above|\$[0-9][0-9,]*.and.over|>.*\$[0-9][0-9,]*'):
                        minimumValue = re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*', re.findall('minimum.*\$[0-9][0-9,]*|above.\$[0-9][0-9,]*|over.*\$[0-9][0-9,]*|greater.than.*\$[0-9][0-9,]*|\$[0-9][0-9,]*.or.more|\$[0-9][0-9,]*.and.above|\$[0-9][0-9,]*.and.over|>.*\$[0-9][0-9,]*', df.loc[i, field], re.IGNORECASE)[0])[0])
                    if patternExists(df, field, i, 'up.to.*\$[0-9][0-9,]*k|below.\$[0-9][0-9,]*k|under.\$[0-9][0-9,]*k|less.than.\$[0-9][0-9,]*k|<.*\$[0-9][0-9,]*k'):
                        maximumValue = re.sub('[kK]', '000', re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*k', re.findall('up.to.*\$[0-9][0-9,]*k|below.\$[0-9][0-9,]*k|under.\$[0-9][0-9,]*k|less.than.\$[0-9][0-9,]*k|<.*\$[0-9][0-9,]*k', df.loc[i, field], re.IGNORECASE)[0], re.IGNORECASE)[0]))
                    elif patternExists(df, field, i, 'maximum.*\$[0-9][0-9,]*|up.to.*\$[0-9][0-9,]*|below.\$[0-9][0-9,]*|under.\$[0-9][0-9,]*|less.than.\$[0-9][0-9,]*|<.\$[0-9][0-9,]*'):
                        maximumValue = re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*', re.findall('maximum.*\$[0-9][0-9,]*|up.to.*\$[0-9][0-9,]*|below.\$[0-9][0-9,]*|under.\$[0-9][0-9,]*|less.than.\$[0-9][0-9,]*|<.*\$[0-9][0-9,]*', df.loc[i, field], re.IGNORECASE)[0])[0])    
                    if patternExists(df, field, i, '\$[0-9][0-9,]*k.{1,4}\$[0-9][0-9,]*k'):
                        text = re.findall('\$[0-9][0-9,]*k.{1,4}\$[0-9][0-9,]*k', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.sub('[kK]', '000', re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*k', text, re.IGNORECASE)[0]))
                        maximumValue = re.sub('[kK]', '000', re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*k', text, re.IGNORECASE)[1]))
                    elif patternExists(df, field, i, '\$[0-9][0-9,]*.{1,4}\$[0-9][0-9,]*|\$[0-9][0-9,]*.to.[0-9][0-9,]*'):
                        text = re.findall('\$[0-9][0-9,]*.{1,4}\$[0-9][0-9,]*|\$[0-9][0-9,]*.to.[0-9][0-9,]*', df.loc[i, field], re.IGNORECASE)[0]
                        minimumValue = re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*', text, re.IGNORECASE)[0])
                        maximumValue = re.sub('[$,]', '', re.findall('\$[0-9][0-9,]*|[0-9][0-9,]*', text, re.IGNORECASE)[1])
                    if minimumValue != None or maximumValue != None:
                        tierValues[unitOfMeasureAltHeader] = 'DOLLAR'
                    else:
                        continue
                    if i in rowsToAdd:
                        row = rowsToAdd.get(i)
                        tierValues[minimumValueAltHeader] = minimumValue if row.get(minimumValueAltHeader) == None else row.get(minimumValueAltHeader)
                        tierValues[maximumValueAltHeader] = maximumValue if row.get(maximumValueAltHeader) == None else row.get(maximumValueAltHeader)
                    else:
                        tierValues[minimumValueAltHeader] = minimumValue
                        tierValues[maximumValueAltHeader] = maximumValue
                        rowsToAdd[i] = tierValues
        except KeyError:
            logger.warning('Cannot check field ' + field + ' for LVR data in an alternate unit of measure as the field is not found in file.')
    return rowsToAdd

def fillLVR(df, rowsToAdd, minimumValueHeader, maximumValueHeader, unitOfMeasureHeader):
    for rowIndex in rowsToAdd:
        row = df.iloc[rowIndex]
        values = rowsToAdd.get(rowIndex)
        minValue = df.loc[rowIndex, minimumValueHeader]
        maxValue = df.loc[rowIndex, maximumValueHeader]
        unitOfMeasure = df.loc[rowIndex, unitOfMeasureHeader]
        if pd.isnull(minValue) and pd.isnull(maxValue):
            df.loc[rowIndex, minimumValueHeader] = values.get(minimumValueHeader)
            df.loc[rowIndex, maximumValueHeader] = values.get(maximumValueHeader)
            df.loc[rowIndex, unitOfMeasureHeader] = values.get(unitOfMeasureHeader)
            continue
        if unitOfMeasure != 'MONTH':
            if pd.isnull(minValue) and values.get(minimumValueHeader) is not None:
                df.loc[rowIndex, minimumValueHeader] = values.get(minimumValueHeader)
                df.loc[rowIndex, unitOfMeasureHeader] = values.get(unitOfMeasureHeader)
            elif pd.isnull(maxValue) and values.get(maximumValueHeader) is not None:
                df.loc[rowIndex, maximumValueHeader] = values.get(maximumValueHeader)
                df.loc[rowIndex, unitOfMeasureHeader] = values.get(unitOfMeasureHeader)
    
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
        print('yo')
        df = pd.read_csv(os.path.join(directoryWithFilesToTag, fileName), encoding='cp1252')
        
    df[tagHeader] = None
    
    # Check for patterns
    logger.info('Checking for patterns specified in config file ' + configFileName + '...')
    patterns = configs['DataTagger'].get('Pattern')
    addNeedToFixTagBasedOnFieldData(df, patterns)

    # Update additional value
    try:
        updateAdditionalValue(df)
    except KeyError:
        logger.warning('Additional value field not found in file. Skipping the update of additional values.')
    
    # Update LVR details
    logger.info('Updating LVR values...')
    fieldsToCheck = configs['DataTagger'].get('LVR').get('fieldsToCheck')
    rowsToAdd = updateLVR(df, fieldsToCheck)
    fillLVR(df, rowsToAdd, minimumValueHeader, maximumValueHeader, unitOfMeasureHeader)
    dollarRowsToAdd = updateLVRAlt(df, fieldsToCheck)
    fillLVR(df, dollarRowsToAdd, minimumValueAltHeader, maximumValueAltHeader, unitOfMeasureAltHeader)
    
    # Add Good tag
    logger.info('Adding Good tags...')
    fieldsToCheck = configs['DataTagger'].get('GoodTag').get('fieldsToCheck')
    goodTagIndices = set(df[lendingRateTypeHeader].index)
    for field in fieldsToCheck:
        goodTagIndices = goodTagIndices - getEmptyIndices(df, field) 
    for i in goodTagIndices:
        df.loc[i, tagHeader] = goodTag
    
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
