# json-to-csv-converter

## Data Extractor
1. Ensure the bank API details are saved in the `products_master.txt` file and run the python script named `script.py`.

## Data Tagger
1. Add your csv files to be tagged to the `tagger` directory and run the `data_tagger.py` script.

## Sheet Uploader
1. Follow the steps given in the Google Cloud documentation for (getting started with authentication)[https://cloud.google.com/docs/authentication/getting-started#auth-cloud-implicit-python] and set up a service account. This service account will be used by the script to authenticate to the Google Sheet.
2. Set an environment variable called `GOOGLE_APPLICATION_CREDENTIALS` with the file path to the service account authentication file.
3. Specify the following configs in the `config.yaml` file.

   - `spreadsheetId` - This is the id of the Google Sheet to be updated. This is mandatory. 
      The id of a Google Sheet can be found in the sheet's url as shown here: `https://docs.google.com/spreadsheets/d/{spreadsheetId}/`
   - `nameOfSheetToBeUpdated` - This is the name of the specific sheet to be updated in our Google Sheet.
   - `uploadMode` - If this is set to `overwrite`, all data in the sheet will be overwritten with the new data. If it is set to `append`, which is the default, only the new lines of data will be appended to the sheet.
   - `historicalData` - If this is set to true, the sheet to be uploaded can be specified using the `pathToSheetToBeUploaded` config described below. 
      By default, this is false, which means the script will look for the tagged script for the date the script is run in the `tagger/tagged` directory in the project. Therefore, ensure the tagged file for the day is available in the `tagged` directory when running the script in the default mode.
   - `pathToSheetToBeUploaded` - This is necessary only if the `historicalData` config is set to true Here, we specify the file path of the sheet to be uploaded.

## Script Runner
- This bash script will run all of the above 3 scripts consecutively.