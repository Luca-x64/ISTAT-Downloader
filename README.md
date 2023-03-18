# ISTAT Downloader

This script allows the user to easily interrogate Istat (Italian National Institute of Statistics) API (https://www.istat.it/en/).
you can choose the Data category (Dataflow - DF) and the filters for this dataflow chosen, then it is possible to load the data obtained by the API, on Google Cloud Storage and then transfer them to a table on Google Big Query.
During the execution of the script you can choose:
- The dataflow ID from the `out/choose_dataflow.txt` file
- Filters from the `Out/Choose_filter.txt` file


# How to use
- import the your .json Google private key 
- look the file config.py and set the `key_path` of your key and the `project` name.
   (it's also possibile edit the `bucket_name` and the `dataset` name)



# Run

run the `main.py` file

or

- import the Istat class: `from main import Istat`

`.choose_dataflow()`          Choose the dataflow

`.get_data()`                 Get data from the ISTAT API

`.loadGstorage()`             Load file csv on Google Cloud Storage

`.loadGBQ()`                  Move the file on Gcs on a Google BigQuery table

Not available
`.get_new_key_position()`     Scan the excel file for getting the new order of the dimensions (keys filters)

`.chooselink()`               Choose endpoint link



while choosing filters you can choose multiple value by separate them with a "+" ex. Y30+Y31+Y32 ...
Choose everything (wildcard) by insert nothing (press enter)


# Useful links
https://www.istat.it/it/metodi-e-strumenti/web-service-sdmx

https://developers.italia.it/it/api/istat-sdmx-rest.html

https://github.com/ondata/guida-api-istat

https://github.com/Attol8/istatapi

https://www.youtube.com/watch?v=0OfsXybrwe

