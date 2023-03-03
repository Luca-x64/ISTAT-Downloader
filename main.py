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

    # Resource location data
    agencyID = ""  # The IT1 domain, maintained by the ISTAT,
    # 22_289       #ID of the resource (see xml/df_ ì{version (old/new/pop)}.xml)
    resourceID = ""
    version = ""
    refID = ""

    query_response = True
    chosen_filter = ''  # Example: A.063039.JAN.9.Y45.99
    chosen_link = ''
    all_filters = {}
    #cols = []
    #cols_type = []

    # Client Google
    storage_client = None
    bq_client = None

    Gtime_filename = 'data'

    def __init__(self):
        pass

    # Choose Dataflow => set resourceID, agencyId, version, refID
    def choose_dataflow(self):
        file_df = "xml/df_old.xml"
        file_choose_df = "out/choose_dataflow.txt"

        if not exists(file_df):
            self.request(DF_OLD, file_df)
            if not self.query_response:
                exit(2)

        xml_parse = ET.parse(file_df)
        dataflows = xml_parse.xpath(
            "//structure:Dataflow", namespaces=namespace)

        df_IDs = []
        with open(file_choose_df, "w") as df_file:
            for df in dataflows:
                df_file.write("[{}] {}\n".format(
                    df.get("id"), df[not lang].text))
                df_IDs.append(df.get("id"))

        df_choose_ID = input(
            "Choose 'Dataflow' (see from {}): ".format(file_choose_df)).strip()

        if df_choose_ID not in df_IDs:
            print(f"Error: Dataflow with id {df_choose_ID} not found")
            exit(1)

        df_selected = xml_parse.xpath(
            f"//structure:Dataflow[@id='{df_choose_ID}']", namespaces=namespace)[0]
        self.resourceID = df_choose_ID
        self.version = df_selected.get("version")
        self.agencyID = df_selected.get("agencyID")
        self.refID = df_selected.find(
            "structure:Structure", namespace).find("Ref").get("id")

    def get_data(self):  # get data from istat api
        if self.query_response:
            self.chosen_link = ALL_LINK[1]  # || self.chooselink()

            self.prepare_filters()  # prepare the filter in the right format of the current dataflow
            print("\n\nrequesting data...")
            self.request(self.chosen_link[-1].format(self.agencyID,  # API request
                         self.resourceID, self.chosen_filter), outputdata)

            self.xml_to_csv()  # convert xml file to csv

    def prepare_filters(self):  # Prepare the filters
        file_available = f"xml/available_keys_{self.resourceID}.xml"
        file_ds = f"xml/ds_{self.resourceID}.xml"
        file_choose_filter = "out/choose_filter.txt"

        link = self.chosen_link
        # ac_link = link[2].format(self.agencyID,self.resourceID,self.version)
        ac_link = AC_OLD.format(self.agencyID, self.resourceID, self.version)

        # Create file
        if not exists(file_available):
            print(f"gathering dataflow of {self.resourceID}...")
            self.request(ac_link, file_available)

            if not self.query_response:
                print(
                    f"The Dataflow with ID={self.resourceID} is not available!", "So search another one!", sep="\n")

                self.choose_dataflow()
                self.prepare_filters()

        ds_link = link[3].format(self.refID)
        if not exists(file_ds):
            print(f"gathering dataflow of {self.resourceID}...")
            self.request(ds_link, file_ds)

        xml_parse = ET.parse(file_ds)  # parse xml file
        new_key_order = xml_parse.xpath(
            f"//structure:DataStructure[@id='{self.refID}']//structure:DimensionList/structure:Dimension//structure:Enumeration/Ref/@id", namespaces=namespace)

        cols_raw = xml_parse.xpath("//structure:DataStructureComponents//structure:ConceptIdentity/Ref/@id", namespaces=namespace)

        user_filters = []

        xml_parse = ET.parse(file_available)  # parse xml file
        codelists = xml_parse.xpath(
            "//structure:Codelist", namespaces=namespace)
        codelists_ID = xml_parse.xpath(
            "//structure:Codelist/@id", namespaces=namespace)

        self.cols = cols_raw[:len(codelists)]

        new_order = []

        # sort the codelists in the right order
        for i in range(len(codelists)):
            new_order.append(codelists[codelists_ID.index(new_key_order[i])])

        for codelist in new_order:
            sub_filter = {}
            for code in codelist.findall("structure:Code", namespaces=namespace):
                sub_filter[code.get("id")] = code[not lang].text
            self.all_filters[codelist.get("id")] = sub_filter

            description = codelist.findall("common:Name", namespaces=namespace)[
                not lang].text

            # Create list of the user keys filters

            if len(sub_filter) == 1:
                sub_choose = next(iter(sub_filter))
                print("choose {}: chosen: '{} [{}]' because it was the only choice".format(
                    description, str(sub_filter.get(sub_choose)).capitalize(), "TODO"))

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

        # formatting the filters for the API
        result_filter = ("{}."*len(codelists))[:-1].format(*user_filters)
        self.chosen_filter = result_filter

    # scan the excel file for getting the new order of the dimensions (keys filters)
    def get_new_key_position(self):  # TODO may in future will be helpful
        if False:
            excel_path = "xlsm/new_dimension.xlsm"

            df = pd.read_excel(
                excel_path, sheet_name="Transcodifica Dimensioni", index_col=20,)

            # get dataframe of the current resourceID (dataflowID)
            df = df[df["DF"] == self.resourceID]
            res = df.reset_index()

            new_position = []
            for index, row in res.iterrows():
                new_position.append(row["POS_NEW"])
            return new_position
        else:
            print("Not yet implemented!")

    def request(self, url, filename):  # Make an API request
        response = http_adapter.get_legacy_session().get(
            url, headers={'content-type': 'application/json'})

        try:
            content = response.content.decode("utf-8")
        except Exception:
            content = response.content
            pass

        if "200" in str(response):
            self.export_file(filename, content)
            self.query_response = True
        else:
            print("status:", response, url)
            print(response.content)
            self.query_response = False

    def xml_to_csv(self):  # Convert the API response file xml to csv
        if self.query_response:

            # cols = ["CITY", "SEX", "AGE", "CIVIL_STATUS", "YEAR", "VALUE"]
            self.cols
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
                                # if i != 0 and i != 2:
                                
                                value_key = obs_value[i].get("value")
                                #type = "STRING"
                                #try:
                                #    float(value_key)
                                #    type = "NUMERIC"
                                #except Exception:
                                #    pass
                                #self.cols_type.append(type)
                                value = self.all_filters.get(
                                    list(self.all_filters.keys())[i]).get(value_key)
                                row.append(value)

                        case "{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic}Obs":
                            final_row = row.copy()
                            for obs in sub:
                                value = obs.get("value")

                                if obs.get("id") not in self.cols:
                                    self.cols.append(obs.get("id"))
                                #
                                #type = "STRING"
                                #try:
                                #    float(value_key)
                                #    type = "NUMERIC"
                                #except Exception:
                                #    pass
                                #self.cols_type.append(type)

                                final_row.append(value)
                            rows.append(final_row)
                            

            # Exporting the csv file
            df = pd.DataFrame(rows, columns=self.cols)
            df.to_csv('out/data.csv')
            print("out/data.xml converted in out/data.csv")
        else:
            exit()

    def loadGstorage(self):  # Load the file csv on Google cloud Storage
        if self.storage_client == None:
            self.G_login()
        self.Gtime_filename += f"_{self.resourceID}" + date.today().strftime("_%m-%d-%y") + \
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

    def loadGBQ(self):  # move file from Google Cloud storage to a table on Google Big Query
        if  self.bq_client == None:
            self.G_login()

        table_name = self.Gtime_filename

        # self.bq_client.create_dataset(dataset)   # create a dataset on G BigQuery

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

        #table_schema = [
        #    bigquery.SchemaField("ID", "INTEGER"),
        #    # TODO responsive
        #    # bigquery.SchemaField("CITTA", "STRING"),
        #    # bigquery.SchemaField("SESSO", "STRING"),
        #    # bigquery.SchemaField("ETA", "STRING"),
        #    # bigquery.SchemaField("STATO_CIVILE", "STRING"),
        #    # bigquery.SchemaField("ANNO", "INTEGER"),
        #    # bigquery.SchemaField("VALORE", "INTEGER"),
        #]
#
        #for schema_field in self.cols[:-1]:
        #    table_schema.append(bigquery.SchemaField(
        #        schema_field, self.cols_type[self.cols.index(schema_field)]))
        #
        #table_schema.append(bigquery.SchemaField("VALUE","NUMERIC"))
        
        # Table data schema
        # job_config = bigquery.LoadJobConfig(schema=table_schema,
        #                                     skip_leading_rows=1,
        #                                     # The source format defaults to CSV, so the line below is optional.
        #                                     source_format=bigquery.SourceFormat.CSV,
        #                                     )
        job_config = bigquery.LoadJobConfig(autodetect=True,source_format=bigquery.SourceFormat.CSV )

        
        uri = f"gs://{bucket_name}/{self.Gtime_filename}.csv"
        load_job = self.bq_client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )  # Make an API request.

        load_job.result()  # Waits for the job to complete.

        # Make an API request.
        destination_table = self.bq_client.get_table(table_id)
        print("File transfered on G BigQuery - Loaded {} rows.".format(destination_table.num_rows))
        self.bq_client.query("ALTER TABLE `"+table_id+"` DROP COLUMN int64_field_0")
        self.bq_client.query("ALTER TABLE `"+table_id+"` RENAME COLUMN int64_field_8 TO VALUE")
        
        
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
        else:
            print("not available")


    def export_file(self, path, content):  # export api response content in xml file

        with open(path, "w") as file:
            file.write(content)

        # formatting the xml file
        x = ET.parse(path)
        pretty_xml = ET.tostring(x, pretty_print=True, encoding=str)
        with open(path, "w") as file:
            file.write(pretty_xml)

        print("File created succesfully '{}'".format(path))

    def query(self,query):
        pass
    
    
if __name__ == "__main__":

    istat = Istat()
    istat.choose_dataflow()
    istat.get_data()
    istat.loadGstorage()
    istat.loadGBQ()

    # istat.create_bucket("istat_population") #No permission
