# EventDrive to Medicare

Process in real time Medicare provider information from US and show insights and a pipeline.

### Solution

In High level data will be extract from Medicare to a csv , then using kafka I will create a kafka topic and move data from csv to kafka using kafka connect, next step is consume it with a python consumer and finally transform and show the data in a dashboard. Additionally from original data i will generate synthetic data from the original and move it into batch to aws s3. All built with container-microservices.

I consider the following technologies for Resolve the problem:

*	Docker-compose and containers
*	Kafka topcis, consumergroups and connect
*	Python Consumerscript -Jupyter
*	S3 bucket
*	Elasticsearch Index
*	Kibana visualizations and dashboards
 
![Image of stack](/img/frankoScienceDiagram.png)

### Up environment:

1. Get sample data(.csv) from https://data.cms.gov/Medicare-Physician-Supplier/Medicare-Provider-Utilization-and-Payment-Data-Phy/fs4p-t5eq/data

2. Run containers( +compose)

>docker run -d --name jupyter3 -e "TZ=America/Lima" -p 1002:8888 jupyter/datascience-notebook

>docker-compose -f kafka-confluent.yml up -d

>docker-compose -f elasticsearch-elastic.yml up -d

3. Move datasource file to connect container: docker cp input1.csv connect: /home/input1.csv

4. Open kafka in web browser http://localhost:9021

>Go to topics > add topic > name ‘sciencesourcetopic’ & create

![Image of topic](/img/createtopic.png)

>Go to connect > click default cluster > upload connector config file> select  ‘connector_fileconnector_config.json’ from config dir & click continue

![Image of connect](/img/kafkaconnect.png)

5. Open jupyter-python in web browser http://localhost:1002

>Open file customElasticsearchIndex.py from app dir and run it for create the elasticsearch index
>Open file getMain.py from app dir and run it for run the main app.

![Image of elasticsearch](/img/FromKafka_ToElasticserach.png)

6. Open dashboard-kibana in web browser http://localhost:5601 and go to dashboards and visualize graphics

![Image of kibana](/img/FrankoDashboard.png)
> KIBANA DASHBOARD: Count by provider, Credentiales per Gender, count service by HCPCS code , Count by StateCode

7. Open aws console and check the batch synthethic data landing in s3

![Image of s3](/img/SyntheticDataS3.png)

### Deep explanation

I started downloading the medicare file, but it is so heavy I splitted to multiple files of 10000010 records per file.

Then I moved to the records to a Kafka topic  , the way that I consider is using Kafka connect because connector are so reliable and easy method to do the things that are already created. Otherwise, I could create a python producer to do it (prefer connect).

Then I was using a python client for kafka I created a consumer who get from kafka topic using a consumer group, I realized some transformations and created some functions for handle data.

I prepared 2 Json(dictionaries) in the consumer, the first one(OriginalData) for store In Elasticsearch and the other one for send minibatch to S3(SyntheticData). These json was transformed before. I used Elasticsearch because Is great for searches and aggregations in realtime instead OLTP db, and S3 because all this data could be consider for big data purposes, object storage are suitable on cloud.

And Finally all docs(json) in Elasticseach Index will be visualized in a kibana dashboard. I chose Kibana as second option my first one was apache superset, but superset documentation says docker for windows is not working just now. Anyway Kibana handle multiple visualizations and supports dashboards in real time, also  allows all elastic querys from Elasticsearch. 


