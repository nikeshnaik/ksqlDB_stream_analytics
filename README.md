# Analysis of Cricket Data (Streaming)

## Objective 

1. Learning data stream processing using Kafka and build a data system.
2. Create infra good enough to mirror Production
3. Use minimum tools, library, packages, framework appropriate for task.
4. The data system consists of 3 data source: yaml + AWS Lambda, CSV + Supabase Postgres Events, Json + MongoDB Atlas.
5. A local process will create a infra using above services using terraform, upload data.
6. Every data will uploaded as single transaction to simulate a production database transaction and events being fired for every new data points.
7. At one point we have a 3 events to be processed in real time aka data stream.
8. Use Kafka locally hosted to store the events in topics and let stream processors handle it.
9. You could use any stream processing tool: Kafka Stream - Python Faust, KSQL or Apache Flink or simple Python Script.
10. Dump into a DuckDb OLAP warehouse
11. Connect a Apache SuperSet, its supports DuckDB connector


