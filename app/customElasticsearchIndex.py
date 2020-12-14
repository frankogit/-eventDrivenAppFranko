# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

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