# Analysis of Cricket Data (Streaming)

## Objective

Build Data Stream Processing and Analytics using Kafka as message broker and Apache SuperSet as frontend. 

## Functional Requirements

- Data sources will generate events everytime a transaction is complete, this events must be consumed by kafka topic and let consumer aware of new events to process them in realtime.
- Each Data source must act as a Producer of events connect with Kafka Producer API.
- Each Data Sink must act as a Consumer of event stream and have a event processing system.
- The processed events must be dumped into a Data Warehouse for Analytics
- Analytics could be a SQL or Python + Visualization library.

## Non Functional Requirements

- The event streams won't be reproducible, system must be aware of it or build around it.
- Data sources can't be considered as ground truth, say Postgres crashes and repeats the events, system must be able to figure it out how to remove them.
- Data Sink must acknowledge after consumption.
- Message Broker - kafka must not be used as Database.
- Analytics must update with new data.

## Technical Approach

-  The data system should consists of 3 data source: yaml + AWS Lambda, CSV + Supabase + Postgres Events, Json + MongoDB Atlas.
-  A local process will create a infra using above services with terraform and a script upload data.
-  Every data will uploaded as single transaction to simulate a production database transaction and events being fired for every new data points.
-  At one point we have a 3 events to be processed in real time aka data stream.
-  Use Kafka locally hosted to store the events in topics and let stream processors handle it.
-  You could use any stream processing tool: Kafka Stream - Python Faust, KSQL or Apache Flink or simple Python Script.
-  Dump into a DuckDb OLAP warehouse
-  Connect a Apache SuperSet, its supports DuckDB connector


## High Level Design - Streaming Analytics

![Alt text](high_level_data_architecture.png)


## Components

#### OLTP System

- Have a python script which will act as application server, every 5 seconds dump a new data record
- JSON object to MongoDB
- CSV row into Supabase Postgres
- AWS Lambda local storage has 10gb limit, create lambda function with terraform, add a python script to iterate every 5 seconds to act as a object storage emitting events.
- Use Terraform to create infra for above OLTP system.

#### Event Broker - Kafka

- Setup Kafka on Docker localhost, which will ingest all producers of OLTP.
- Events will available in topics

#### Stream Processing

- Once data is available in Kafka topics, either use a consumer to process the events and dump to another topic or use KSQL for stream processing.
- For v1, a simple Python Script and KSQL.

#### Data Warehouse and Analytics

- A kafka consumer will dump data into DuckDb to store processed streams
- Apache Superset connect to warehouse and perform analyses on data.

## Data

- Json -> T20 Internationals-men
- CSVs -> One day Matches
- Yaml -> Test Matches

## Version 1

- MongoDB Json + MongoDB Atlas without terraform
- Kafka Local Setup
- Python Script to processing stream by using Kafka Connector API
- DuckDB to store processed stream
- Apache SuperSet


## Tasks

- [x] MongoDB Atlas project creation, manually.
- [x] Python Script as Application code to dump Json object to Atlas
- [x] Kafka setup on local as containers
- [x] Python Script to connect MongoDB stream to Kafka Producer API
- [x] Python Script with Kafka Consumer API as Stream Processor to enhance and dump to Warehouse OLAP
- [x] Split source mongo topic into 3 topics: equivalent of datasets and dump to DuckDb
- [x] Transform Json into Table Record
- [x] DuckDB creation
- [ ] Apache Superset Docker
- [x] End to End system
- [ ] Extra: Testing in stream processing??

## Analysis:

#### Topic Segregation 
- Extract Match meta data into a topic
- Extract Player data into a topic
- Extract Innings Data into a topic
- One topic from Mongo stream processed into 3 topics using Python Script

![Alt text](topic_segregation.png)

## How to Run on Local

Create Mongo Atlas Account on their platform, use default cluster0, create a database user, whitelist your own IP in network access, get Mongo URI and create + append in .env file

> `MONGO_URI=mongodb+srv://<user>:<password>@<cluster_name>.mongodb.net`

Install Python dependencies:

> `pip install -r requirements.txt`

Start Kafka Local Docker container, creates DuckDB OLAP file:

> `make local-infra-whirl-up`

Run whole system for 5mins as demo [ Resource constraint]

> `python main.py`

Check logs
> `tail -f system.log`

Load Enhanced data into DuckDB
> `python -m data_warehouse.load`



