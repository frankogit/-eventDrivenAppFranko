#!/usr/bin/env python
# coding: utf-8

# In[28]:


#!/usr/bin/env python
import os
from confluent_kafka import Consumer
import time
import json
import re
import csv
import mimetypes
from faker import Faker
import random
import re
from elasticsearch import Elasticsearch
import uuid
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
import pandas as Dataframe

####################################################################### 
# KAFKA CONSUMER MICROSERVICE
# FRANKO ORTIZ
# KAFKA CONFLUENT PYTHON CLIENT

####################################################################### 
# FAKER + RANDOM FOR CREATE SYNTHETIC FROM SOURCE KAFKA TOPIC
fake = Faker()

c = Consumer({
    'bootstrap.servers': 'broker:9092',
    'group.id': 'consumer9',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['sciencesourcetopic'])


es = Elasticsearch(
    ['es01:9200', 'es02:9200','es03:9200'],    
    sniff_on_start=True,                  # sniff before doing anything    
    sniff_on_connection_fail=True,        # refresh nodes after a node fails to respond    
    sniffer_timeout=60)                   # and also every 60 seconds

####################################################################### 
# USER DEFINE FUNCTIONS

def random_nat_prov_id(rng):
    prov_id=''
    for _ in range(rng):
        prov_id = prov_id + str(random.randrange(1, 10, 1))
    return prov_id

def random_medical_numbers(maxx, param,number_type):

    try:
        default_number=float(param)
    except:
        default_number=int(maxx)
    
    if maxx <= default_number :
        maxx= default_number

    if number_type=='float':
        default_number=random.uniform(1, maxx)
    else:
        default_number=int(random.uniform(1, maxx))
        
    return maxx,default_number

def upload_to_aws(local_file, bucket, s3_file):
    
    os.environ['AWS_PROFILE'] = "default"
    # Let's use Amazon S3
    s3 = boto3.client('s3')

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

#######################################################################    
# lOCAL VARIABLES

List_json_to_es=[]
List_credentials=[]
List_city_code=[]
List_provider_type=[]
List_hcpcs_detail=[]

List_gender=['F','M']
List_entity_type=['I','O']
List_medic_partic_ind=['Y','N']
List_hspcs_drug=['Y','N']
List_plac_serv=['F','O']

count_bulk_s3=0

max_nmbr_services=0.0
nmbr_services=0
max_nmbr_medic_benef=0.0
nmbr_medic_benef=0
max_nmbr_dmedic_benef_pds=0.0
nmbr_dmedic_benef_pds=0
max_avg_medic_aa=0.0
avg_medic_aa=0.0
max_avg_subm_ca=0.0
avg_subm_ca=0.0
max_avg_subm_pa=0.0
avg_subm_pa=0.0
max_avg_subm_sa=0.0
avg_subm_sa=0.0

#######################################################################  
# EVENTS COMING

while True:
    start_time = time.time()
    msg = c.poll(1.0) # GET MESSAGES

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue
        
    msg=format(msg.value())
    main_event=msg[msg.find('\"'):-1] # SLICING EVENT SINCE NO HAVE KAFKA KEY
    main_event=re.sub(r'(?!(([^"]*"){2})*[^"]*$),', '', main_event) # REMOVE COMMAS IN FIELDS
    lst_sptd=main_event.split(',') #  FLAT TEXT TO PYTHON LIST
    
    # POPULATING MEDICARE TO ELASTICSEARCH
    
    doc_es = {
        "National_Provider_Identifier": lst_sptd[0].strip('"'),
        "Last_Name_Organization_Name_Provider": lst_sptd[1].strip('"'),
        "First_Name_of_the_Provider": lst_sptd[2].strip('"'),
        "Middle_Initial_of_the_Provider":lst_sptd[3].strip('"'),
        "Credentials_of_the_Provider": lst_sptd[4].strip('"'),
        "Gender_of_the_Provider": lst_sptd[5].strip('"'),
        "Entity_Type_of_the_Provider": lst_sptd[6].strip('"'),
        "Street_Address_1_of_the_Provider": lst_sptd[7].strip('"'),
        "Street_Address_2 of_the_Provider": lst_sptd[8].strip('"'),
        "City_of_the_Provider": lst_sptd[9].strip('"'),
        "Zip_Code_of_the_Provider": lst_sptd[10].strip('"'),
        "State_Code_of_the_Provider": lst_sptd[11].strip('"'),
        "Country_Code_of_the_Provider":lst_sptd[12].strip('"'),
        "Provider_Type": lst_sptd[13].strip('"'),
        "Medicare_Participation Indicator": lst_sptd[14].strip('"'),
        "Place_of_Service": lst_sptd[15].strip('"'),
        "HCPCS_Code": lst_sptd[16].strip('"'),
        "HCPCS_Description": lst_sptd[17].strip('"'),
        "HCPCS_Drug_Indicator": lst_sptd[18].strip('"'),
        "Number_of_Services": lst_sptd[19].strip('"'),
        "Number_of_Medicare_Beneficiaries":lst_sptd[20].strip('"'),
        "Number_of_Dist_Med_Beny_Day_Serv": lst_sptd[21].strip('"'),
        "Average_Medicare_Allowed_Amount": lst_sptd[22].strip('"'),
        "Average_Submitted_Charge_Amount": lst_sptd[23].strip('"'),
        "Average_Medicare_Payment_Amount": lst_sptd[24].strip('"'),
        "Average_Medicare_Stand_Amount": lst_sptd[25].strip('"'),
        "load_date_Elasticsearch": datetime.now()
    }
    res = es.index(index="test-index7", id=uuid.uuid1(), body=doc_es)
    print(res['result'])

    #res = es.get(index="test-index", id=1)
    #print(res['_source'])
    
    # CALCULATING SYNTHETIC DATA ON THE FLIGHT, WITHOUT GAP TRAINNING
    # THEN SEND TO S3
    
    List_credentials.append(lst_sptd[4]) if lst_sptd[4] not in List_credentials else List_credentials
    List_city_code.append(lst_sptd[11]) if lst_sptd[11] not in List_city_code else List_city_code
    List_provider_type.append(lst_sptd[13]) if lst_sptd[13] not in List_provider_type else List_provider_type
    List_hcpcs_detail.append(lst_sptd[16]+'-'+lst_sptd[17]) if lst_sptd[16]+'-'+lst_sptd[17] not in List_hcpcs_detail else List_hcpcs_detail

    max_nmbr_services,nmbr_services= random_medical_numbers(max_nmbr_services,lst_sptd[19].strip('"'),'int')    
    max_nmbr_medic_benef,nmbr_medic_benef= random_medical_numbers(max_nmbr_medic_benef,lst_sptd[20].strip('"'),'int')
    max_nmbr_dmedic_benef_pds,nmbr_dmedic_benef_pds= random_medical_numbers(max_nmbr_dmedic_benef_pds,lst_sptd[21].strip('"'),'int')    
    max_avg_medic_aa,avg_medic_aa= random_medical_numbers(max_avg_medic_aa,lst_sptd[22].strip('"'),'float')    
    max_avg_subm_ca,avg_subm_ca= random_medical_numbers(max_avg_subm_ca,lst_sptd[23].strip('"'),'float')    
    max_avg_subm_pa,avg_subm_pa= random_medical_numbers(max_avg_subm_pa,lst_sptd[24].strip('"'),'float')    
    max_avg_subm_sa,avg_subm_sa= random_medical_numbers(max_avg_subm_sa,lst_sptd[25].strip('"'),'float')

    
    doc_s3 = {
        "National_Provider_Identifier": random_nat_prov_id(10),
        "Last_Name_Organization_Name_Provider": fake.last_name().upper(),
        "First_Name_of_the_Provider": fake.first_name().upper(),
        "Middle_Initial_of_the_Provider":fake.first_name().upper()[0],
        "Credentials_of_the_Provider": random.choice(List_credentials),
        "Gender_of_the_Provider": random.choice(List_gender),
        "Entity_Type_of_the_Provider": random.choice(List_entity_type),
        "Street_Address_1_of_the_Provider": fake.street_address().upper(),
        "Street_Address_2 of_the_Provider": fake.street_address().upper(),
        "City_of_the_Provider": fake.city().upper(),
        "Zip_Code_of_the_Provider": fake.postcode().upper()+random_nat_prov_id(4),
        "State_Code_of_the_Provider": random.choice(List_city_code),
        "Country_Code_of_the_Provider":fake.country_code().upper(),
        "Provider_Type": random.choice(List_provider_type),
        "Medicare_Participation Indicator": random.choice(List_medic_partic_ind),
        "Place_of_Service": random.choice(List_plac_serv),
        "HCPCS_Detail": random.choice(List_hcpcs_detail),        
        "HCPCS_Drug_Indicator": random.choice(List_hspcs_drug),
        "Number_of_Services": nmbr_services,
        "Number_of_Medicare_Beneficiaries":nmbr_medic_benef,
        "Number_of_Dist_Med_Beny_Day_Serv": nmbr_dmedic_benef_pds,
        "Average_Medicare_Allowed_Amount": avg_medic_aa,
        "Average_Submitted_Charge_Amount": avg_subm_ca,
        "Average_Medicare_Payment_Amount": avg_subm_pa,
        "Average_Medicare_Stand_Amount": avg_subm_sa,
        "load_date_s3": datetime.now()
    }
        
    List_json_to_es.append(doc_s3)    
    count_bulk_s3 = count_bulk_s3 + 1
    
    
    # POPULATING TO S3
    if count_bulk_s3 %10 == 0:
        name_file_s3 = 'science_'+str(uuid.uuid1())+'.csv'
        
        df_s3 = pd.DataFrame(data=List_json_to_es)
        df_s3.to_csv(name_file_s3,index=False,sep=',',na_rep='',quoting=csv.QUOTE_ALL,encoding='utf-8')
        
        uploaded = upload_to_aws(name_file_s3, 'sciencebucketassesment', name_file_s3)
        count_bulk_s3 = 0         
        List_json_to_es = []

    #check if mail is in list
    print("Message received in {} seconds.".format(time.time() - start_time))
    print("")
    time.sleep(1)

c.close()





# In[74]:

# CREATE CUSTOM ELASTICSEARCH INDEX

from elasticsearch import Elasticsearch
es = Elasticsearch(
    ['es01:9200', 'es02:9200','es03:9200'],
    # sniff before doing anything
    sniff_on_start=True,
    # refresh nodes after a node fails to respond
    sniff_on_connection_fail=True,
    # and also every 60 seconds
    sniffer_timeout=60)

doc_i={
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "National_Provider_Identifier": { "type": "text", "fields": {"keyword": { "type": "keyword" }}},
      "Last_Name_Organization_Name_Provider": { "type": "text" },
        "First_Name_of_the_Provider": { "type": "text" },
        "Middle_Initial_of_the_Provider": { "type": "text" },
        "Credentials_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Gender_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Entity_Type_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Street_Address_1_of_the_Provider": { "type": "text" },
        "Street_Address_2 of_the_Provider": { "type": "text" },
        "City_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Zip_Code_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "State_Code_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Country_Code_of_the_Provider": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Provider_Type": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Medicare_Participation Indicator": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Place_of_Service": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "HCPCS_Code": { "type": "text", "fields": {"keyword": { "type": "keyword" }} },
        "HCPCS_Description": { "type": "text" },
        "HCPCS_Drug_Indicator": { "type": "text" , "fields": {"keyword": { "type": "keyword" }}},
        "Number_of_Services": { "type": "integer" },
        "Number_of_Medicare_Beneficiaries": { "type": "integer" },
        "Number_of_Dist_Med_Beny_Day_Serv": { "type": "integer" },
        "Average_Medicare_Allowed_Amount": { "type": "float" },
        "Average_Submitted_Charge_Amount": { "type": "float" },
        "Average_Medicare_Payment_Amount": { "type": "float" },
        "Average_Medicare_Stand_Amount": { "type": "float" },
        "load_date_Elasticsearch": { "type": "date" }
        
    }
  }
}

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index='test-index7', ignore=400,body=doc_i)




# In[21]:

# SPLIT 2GB CSV FILE INTO CHUNKS

import pandas as pd
import csv
in_csv = r'C:\Users\franko\Downloads\science-soucedata\medicare.csv'

#get the number of lines of the csv file to be read

number_lines = sum(1 for row in (open(in_csv)))
number_lines=10000000

#size of rows of data to write to the csv, 

#you can change the row size according to your need

rowsize = 1000000
#start looping through data writing it to a new file for each set

for i in range(1,number_lines,rowsize):

    df = pd.read_csv(in_csv,
          header=None,

          nrows = rowsize,#number of rows to read at each loop

          skiprows = i)#skip rows that have been read


    #csv to write data to a new file with indexed name. input_1.csv etc.
    df.columns = ['National Provider Identifier','Last Name/Organization Name of the Provider','First Name of the Provider','Middle Initial of the Provider','Credentials of the Provider','Gender of the Provider','Entity Type of the Provider','Street Address 1 of the Provider','Street Address 2 of the Provider','City of the Provider','Zip Code of the Provider','State Code of the Provider','Country Code of the Provider','Provider Type','Medicare Participation Indicator','Place of Service','HCPCS Code','HCPCS Description','HCPCS Drug Indicator','Number of Services','Number of Medicare Beneficiaries','Number of Distinct Medicare Beneficiary/Per Day Services','Average Medicare Allowed Amount','Average Submitted Charge Amount','Average Medicare Payment Amount','Average Medicare Standardized Amount']
    
    out_csv = r'C:\Users\franko\Downloads\science-soucedata\input' + str(i) + '.csv'
    

    
    df.to_csv(out_csv,

          index=False,

          header=True,

          mode='a',#append data to csv file

          chunksize=rowsize,
          quoting=csv.QUOTE_ALL,
              encoding = 'utf-8'
             )#size of data to append for each loop



# In[ ]:




