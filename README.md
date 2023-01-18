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

#### Event Streaming - Kafka

- Setup Kafka on Docker localhost, which will ingest all producers of OLTP.
- Todo

#### Stream Processing

- Once data is available in Kafka topics, all consumer must process the stream.
- For v1, a simple Python Script should do the work.

#### Data Warehouse and Analytics

- Use DuckDb to store processed streams
- Apache Superset connect to warehouse and perform analyses on data.

## Data

- Json -> T20 Internationals-men
- CSVs -> One day Matches
- Yaml -> Test Matches

## Version 1

- MongoDB Json + MongoDB Atlas without terraform
- Kafka Local 
- Python Script to processing stream by using Kafka Connector API
- DuckDB to store processed stream
- Apache SuperSet


## Tasks

- [x] MongoDB Atlas project creation, manually.
- [x] Python Script as Application code to dump Json object to Atlas
- [ ] Kafka setup on local as containers
- [ ] Python Script with Kafka Connector API as Stream Processor with enhance or dump to Warehouse OLAP
- [ ] DuckDB creation
- [ ] Apache Superset Docker
- [ ] End to End connection
- [ ] Testing in stream processing??

