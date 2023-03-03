from config import *
from constants import *
import HttpAdapter as http_adapter
import pandas as pd
import xml.etree.ElementTree as Xet
from lxml import etree as ET
from datetime import datetime, date

from google.cloud import storage, bigquery
from google.oauth2 import service_account

from os.path import exists
from os import remove


class Istat():

    #Resource location data
    agencyID = ""            #The IT1 domain, maintained by the ISTAT,
    resourceID = "" #22_289       #ID of the resource (see xml/df_ ì{version (old/new/pop)}.xml)
    version = "" 
    refID=""

    query_response = True
    chosen_filter = ''  # Example: A.063039.JAN.9.Y45.99
    chosen_link = ''
    all_filters = {}

    # Client Google
    storage_client = None
    bq_client = None

    Gtime_filename = 'data'

    def __init__(self):
        self.G_login()  # Google service Auth
        self.choose_dataflow() #TODO Spostare?


    def choose_dataflow(self):  #Choose Dataflow => set resourceID, agencyId, version
        file_df = "xml/df_old.xml"
        file_choose_df = "out/choose_datafow.txt"
        
        if not exists(file_df):
            self.request(DF_OLD, file_df)
        
        xml_parse = ET.parse(file_df)
        dataflows = xml_parse.xpath("//structure:Dataflow",namespaces=namespace)
        
        df_IDs = []
        with open(file_choose_df,"w") as df_file:
            for df in dataflows:
                df_file.write("[{}] {}\n".format(df.get("id"),df[not lang].text))
                df_IDs.append(df.get("id"))
        
        df_choose_ID = input(
                    "Choose 'Dataflow' (see from {}): ".format(file_choose_df)).strip()
        
        if df_choose_ID not in df_IDs:
            print(f"Error: Dataflow with id {df_choose_ID} not found")
            exit(1)
        
        df_selected = xml_parse.xpath(f"//structure:Dataflow[@id='{df_choose_ID}']",namespaces=namespace)[0]
        self.resourceID=df_choose_ID
        self.version=df_selected.get("version")
        self.agencyID=df_selected.get("agencyID")
        self.refID=df_selected.find("structure:Structure",namespace).find("Ref").get("id")
    
    def get_data(self):  # get data from istat api
        
        self.chosen_link = ALL_LINK[1]  # || self.chooselink() //TODO

        self.prepare_filters()

        print("\n\nrequesting data...")
        self.request(self.chosen_link[-1].format(self.agencyID,
                     self.resourceID, self.chosen_filter), outputdata)

        self.xml_to_csv()
        self.loadGstorage()
        self.loadGBQ()



    def prepare_filters(self):  # Prepare the filters
        file_available = f"xml/available_keys_{self.resourceID}.xml"
        file_ds = "xml/ds_new.xml"
        file_choose_filter = "out/choose_filter.txt"    
        
        link = self.chosen_link
        ac_link = AC_OLD.format(self.agencyID,self.resourceID,self.version) #ac_link = link[2].format(self.agencyID,self.resourceID,self.version)

        # Create file
        if not exists(file_available):
            print(f"gathering dataflow of {self.resourceID}...")            
            self.request(ac_link, file_available)
            
            if not self.query_response:
                print(f"The Dataflow with ID={self.resourceID} is not available!","So search another one!",sep="\n")
                
                self.choose_dataflow()
                self.prepare_filters()
                
        #TODO datastructure dimensionlist[0]/ refID
        ds_link = link[3].format(self.refID)
        if not exists(file_ds):
            print(f"gathering dataflow of {self.resourceID}...")            
            self.request(ds_link, file_ds)
        
        
        xml_parse = ET.parse(file_ds)  # parse xml file
        new_key_order = xml_parse.xpath(
            f"//structure:DataStructure[@id='{self.refID}']//structure:DimensionList/structure:Dimension//structure:Enumeration/Ref/@id",namespaces=namespace)
        
        user_filters = []

        xml_parse = ET.parse(file_available)  # parse xml file
        codelists = xml_parse.xpath("//structure:Codelist",namespaces=namespace)
        #for c in codelists:
        #    print(c.get("id"))
       
       
        
        #NEW_KEY_ORDER è una lista di parole
        new_order = []
        for i in range(len(codelists)):
           new_order.append(codelists[new_key_order[i]])
        print(new_key_order)
        ##END
        #
        #new_order = codelists
        
        
        #for codelist in new_order:  # Scan the file for the filter options
        #for codelist in codelists:  # Scan the file for the filter options
        for codelist in new_key_order #TODO:  # Scan the file for the filter options      #TODO deve essere una list di dataflow(?) (btw Element xml)
            sub_filter = {}
            for code in codelist:
                if "Name" not in code.tag:
                    sub_filter[code.get("id")] = code[not lang].text
            self.all_filters[codelist.get("id")] = sub_filter
            
            description = codelist[not lang].text

            if len(sub_filter) == 1:
                sub_choose = next(iter(sub_filter))
                print("choose {}: chosen '{}' because it was the only choice".format(
                    description, str(sub_filter.get(sub_choose)).capitalize()))

            else:
                with open(file_choose_filter, "w") as choose_file:
                    for filter_key, filter_value in sub_filter.items():
                        choose_file.write("[{}] {}\n".format(
                            filter_key, filter_value))

                # Choose multiple value by separate them with a "+" ex. Y30+Y31+Y32 ...
                # Choose everything (wildcard) by insert nothing (press enter)
                sub_choose = input(
                    "choose '{}' option (see from out/choose_filter.txt): ".format(description)).strip()

                if "+" in sub_choose:
                    splitted = sub_choose.split("+")

                    result_filter = []
                    for key in splitted:
                        if key in sub_filter.keys():
                            result_filter.append(key)

                    filter_format = ""
                    for sub_filter in result_filter:
                        filter_format += sub_filter+"+"
                    sub_choose = filter_format[:-1]

                else:

                    sub_choose = sub_choose if sub_choose in sub_filter.keys() else ""

            user_filters.append(sub_choose)

        # remove choose_filter file
        if exists(file_choose_filter):
            remove(file_choose_filter)

        # formatting the filter for the API
        result_filter = ("{}."*len(codelists))[:-1].format(*user_filters)
        self.chosen_filter = result_filter
      
    
    def get_new_key_position(self): #test
        excel_path="xlsm/new_dimension.xlsm"
        
        df = pd.read_excel(excel_path,sheet_name="Transcodifica Dimensioni",index_col=20,)

        df = df[df["DF"]==self.resourceID] #get dataframe of the current resourceID (dataflowID)
        res = df.reset_index()

        new_position=[]
        for index,row in res.iterrows():
            new_position.append(row["POS_NEW"])
        return new_position 
        

    def request(self, url, filename):
        print(url)
        response = http_adapter.get_legacy_session().get(
            url, headers={'content-type': 'application/json'})

        content = response.content.decode("utf-8")

        if "200" in str(response):
            self.export_file(filename, content)
            self.query_response=True
        else:
            print("status:", response)
            print(response.content) #DEBUG
            self.query_response = False

    def xml_to_csv(self):  # Convert the API response file xml to csv
        if (self.query_response):

            cols = ["CITY", "SEX", "AGE", "CIVIL_STATUS", "YEAS", "VALUE"]
            rows = []

            data_parse = Xet.parse(outputdata)
            dataset = data_parse.getroot()[1]
            for series in dataset:
                row = []
                for sub in series:
                    match(sub.tag):

                        case "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}SeriesKey":
                            obs_value = sub.findall("generic:Value", namespace)
                            for i in range(len(obs_value)):

                                # insert the first 4 column
                                if i != 0 and i != 2:
                                    value_key = obs_value[i].get("value")
                                    value = self.all_filters.get(
                                        list(self.all_filters.keys())[i]).get(value_key)

                                    row.append(value)

                        case "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs":
                            final_row = row.copy()
                            for obs in sub:
                                final_row.append(obs.get("value"))
                            rows.append(final_row)

            # Exporting the csv file
            df = pd.DataFrame(rows, columns=cols)
            df.to_csv('out/data.csv')
            print("out/data.xml converted in out/data.csv")
        else:
            exit()

    def loadGstorage(self):
        self.Gtime_filename +=f"_{self.resourceID}" + date.today().strftime("_%m-%d-%y") + \
            datetime.now().strftime("_%H-%M-%S")
        # filename on Gcloud
        destination_blob_name = f"{self.Gtime_filename}.csv"

        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0

        blob.upload_from_filename(
            GC_file, if_generation_match=generation_match_precondition)

        print(
            f"File {GC_file} uploaded to {destination_blob_name} on G Cloud Storage."
        )


    def create_bucket(self, bucket_name):  # Create bucket on G Cloud Storage

        new_bucket = self.storage_client.create_bucket(
            bucket_name, location="europe-west8")

        print(
            "Created bucket {} in {} with storage class {}".format(
                new_bucket.name, new_bucket.location, new_bucket.storage_class
            )
        )


    def loadGBQ(self):
        table_name = self.Gtime_filename

        # self.bq_client.create_dataset(dataset)   # create a dataset on G bq

        # Print all datasets available

        # datasets = list(self.bq_client.list_datasets())

        # if datasets:
        #    print("Lista dei dataset disponibili:")
        #    for dataset in datasets:
        #        print("\t{}".format(dataset.dataset_id))
        # else:
        #    print("Non ci sono dataset disponibili.")

        # table_id = "your-project.your_dataset.your_table_name"

        table_id = "{}.{}.{}".format(project, dataset, table_name)

        # Table data schema
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("ID", "INTEGER"),
                bigquery.SchemaField("CITTA", "STRING"),
                bigquery.SchemaField("SESSO", "STRING"),
                bigquery.SchemaField("ETA", "STRING"),
                bigquery.SchemaField("STATO_CIVILE", "STRING"),
                bigquery.SchemaField("ANNO", "INTEGER"),
                bigquery.SchemaField("VALORE", "INTEGER"),
            ],
            skip_leading_rows=1,
            # The source format defaults to CSV, so the line below is optional.
            source_format=bigquery.SourceFormat.CSV,
        )

        uri = f"gs://{bucket_name}/{self.Gtime_filename}.csv"
        load_job = self.bq_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        # Make an API request.
        destination_table = self.bq_client.get_table(table_id)
        print("File trasfered on G BigQuery - Loaded {} rows.".format(destination_table.num_rows))


    def G_login(self):  # Auth with service in Google Cloud Storage and in Google BigQuery

        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=[
                "https://www.googleapis.com/auth/cloud-platform"],
        )

        self.storage_client = storage.Client(
            credentials=credentials, project=credentials.project_id,)
        self.bq_client = bigquery.Client(
            credentials=credentials, project=credentials.project_id,)


    def chooselink(self):  # Not available
        print("not available")
        if False:
            links = [OLD[0], NEW[0], POP[0]]
            # links [:-1] because URL_POP is not complete (missing docs)
            for link in links[:-1]:
                print("[{}] {}".format(links.index(link)+1, link), end="\n")

            choose = int(input("Choose link (default is 1): "))-1

            try:
                selected_links = ALL_LINK[:-1][choose]
            except IndexError:
                selected_links = ALL_LINK[1]

            print(selected_links)


    def export_file(self, path, content):  # export api response content in xml file
        
        with open(path, "w") as file:
            file.write(content)

        x = ET.parse(path)  # formatting the xml file
        pretty_xml = ET.tostring(x, pretty_print=True, encoding=str)
        with open(path, "w") as file:
            file.write(pretty_xml)

        print("File created succesfully '{}'".format(path))

    


istat = Istat()

#istat.choose_dataflow()
# istat.create_bucket("istat_population") #No permission

istat.get_data()
# istat.get_data()
