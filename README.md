# Coding Exercise: Streaming Data

   The exercise is meant to be more freeform: think hackday instead of final exam. The objective here is to do something interesting or useful. You should spend about four hours or so on it.
The goal is not to ship a product, but rather to show you can.
Start with python and kafka, adding any additional tools or libraries you like - for example, Faust.
Just make sure we have a way to run them (and documented, reasonable steps for the "how" part). Submission is by tarball or link to a git repo. Many candidates use docker or docker-compose to run their submission.

For this exercise, we'll be using a public dataset from Centers for Medicare and Medicaid
Services, CMS:

https://data.cms.gov/Medicare-Physician-Supplier/Medicare-Provider-Utilization-and-Payment-Data-Phy/fs4p-t5eq/data

   You can browse the dataset at the link and download it under the "Export" tab on the right. In
.csv format it's about 2.2 GiB, but choose whatever format you find most ergonomic.

   This is the data you will stream into kafka for your exercise. We make extensive use of kafka with streaming data in an event-driven architecture. We encourage you to think about the problem in terms of streams and processors.

   There are a number of active questions in healthcare about utilization and payments. For example, how do costs and utilization vary:

* Within a region
* Across regions
* With treatment level (e.g. varying lengths of patient visits, in-patient vs out-patient)

   Choose a few metrics or statistics to compute. For example, how many regions is a procedure done in? Which procedures have the fewest providers? How much does the cost of a
procedure vary? Basically, feel free to slice and dice the data a little. It'll give you more to talk about during the technical interview.

There are a number of options you might consider (choose one):
1. Streaming Data: Provide metrics on an infinite stream of data. Stream records directly (without transformation) into kafka from the dataset. Use the existing data to create
reasonable synthetic data (random but reasonable values) as well so that you can simulate an infinite stream of new data. Create a consumer or stream processor (see Faust)  that reads the incoming data, transforms it, and prints your metrics or statistics every so often to the terminal.
2. Dashboard: Create a metrics dashboard. Stream records directly (without transformation) into kafka from the dataset. Create a consumer or stream processor (see Faust) that reads the incoming data, transforms it, and pushes it into a data store of some kind (postgres, rocksdb, etc). Use this data store to power a simple dashboard of metrics. (Plotly's Dash is a convenient and commonly used tool for quickly building dashboards.) You are welcome to slow the incoming data and permute its order if you would like to make your dashboard appear to update.
3. Your own adventure: if you have an idea (using python and kafka) that you think is fun or interesting, we are not here to get in your way. Check in with us about it to make sure we're all on the same page about it being reasonable and then have at it. We will discuss your submission in the technical interview. In particular, we will talk about your
approach, any interesting decisions you made or tools you used, and how you might build upon your submission for use in the real world. The exercise is somewhat open ended to allow you a certain amount of creativity. It is not meant to consume too much of your time. If you feel you will exceed the recommended time, let us know and we will add stipulations to constrain the exercise to something that makes sense.

## Solution EventDrive Medicare - Franko ortiz

Process in real time Medicare provider information from US and show insights and a pipeline.

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

1. Get sample data(.csv) from:

https://data.cms.gov/Medicare-Physician-Supplier/Medicare-Provider-Utilization-and-Payment-Data-Phy/fs4p-t5eq/data

2. Run containers( +compose) using files from docker dir

>docker run -d --name jupyter3 -e "TZ=America/Lima" -p 1002:8888 jupyter/datascience-notebook

>docker-compose -f kafka-confluent.yml up -d

>docker-compose -f elasticsearch-elastic.yml up -d

3. Move datasource file to connect container: 

>docker cp input1.csv connect: /home/input1.csv

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


