# json-to-csv-converter

## Data Extractor
1. Ensure the bank API details are saved in the `products_master.txt` file and run the python script named `script.py`.

## Data Tagger
1. Add your csv files to be tagged to the `tagger` directory and run the `data_tagger.py` script.

## Sheet Uploader
1. Set an environment variable `GOOGLE_APPLICATION_CREDENTIALS` with the file path to the service account authentication file.
2. Ensure the tagged file for the day is available in the `tagged` directory and run the script.

## Script Runner
- This bash script will run all of the above 3 scripts consecutively.