# istat

This script allows the user to easily interrogate Istat API (https://www.istat.it/en/), you can choose the
Data category (Dataflow - DF) and the filters for that particular dataflow chosen, subsequently it is possible to load the data obtained by the API on Google Cloud Storage and then transfer them to a table on Google Big Query
During the execution of the script you can choose:
- The dataflow ID from the `out/choose_dataflow.txt` file
- Filters from the `Out/Choose_filter.txt` file


# How to use
- import the your .json Google private key 
- look the file config.py and set the `key_path` of your key , set the `bucket_name` and the `project` and the `dataset`



# Run

run the `main.py` file

or

- Importa la classe Istat : `from main import Istat`

`.choose_dataflow()`          Choose the dataflow
`.get_data()`                 Get data from the ISTAT API
`.loadGstorage()`             Load file csv on Google Cloud Storage
`.loadGBQ()`                  Move the file on Gcs on a Google BigQuery table

Not available
`.get_new_key_position()`     Scan the excel file for getting the new order of the dimensions (keys filters)
`.chooselink()`               Choose endpoint link

