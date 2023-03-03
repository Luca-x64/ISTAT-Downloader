##Program configuration

#Path of the Google Key (Service Account)
key_path = "Gkey/kso-it-sandbox-66cf22ed4ea0.json"

# output data lang
languages = {"en": 0, "it": 1}
lang = languages.get("it") # "it"/"en" change the get parametr


#output file path without format (Ex: .xml)
outputdata = "out/data.xml" #if change, edit also 

#Google Cloud Storage 
bucket_name = "istat_it"    #name of the bucket
GC_file="out/data.csv"              #file to load on Gcloud

#Google Big Query

project = "kso-it-sandbox"      #Google Big query project name
dataset = "istat"         

#Data Namespace dont touch
namespace = {
       'generic': "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic",
       'structure': "http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
   }









# helpful links
#   https://www.istat.it/it/metodi-e-strumenti/web-service-sdmx
#   https://developers.italia.it/it/api/istat-sdmx-rest.html
#   https://github.com/ondata/guida-api-istat
#   https://github.com/Attol8/istatapi
#   https://www.youtube.com/watch?v=0OfsXybrweI
