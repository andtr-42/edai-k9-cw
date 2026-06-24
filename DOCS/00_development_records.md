
1. Docker Optimization 
- Document technique and impact

2. Offline Data Generation 

Document problem characteristics like (validation):
- Skew => print out all the category
- Cardinality => print out unique id
- Schema evolution => print out nulls in old partitions

Document stored data chars => data volumne, data format 
Document config generator

3. Streaming data 

Document problem characteristics like (validation):
- Count the event in burst and normal 
- Print out late arrivals events 

4. Spark Job handling offline data problems 

Base line without optimization

Handle skew with explanation 
- (Problems -> See problems in Spark UI -> I have tried these experiment methods -> After experiment, it increase it performance)

- Show picture of Spark UI with screenshots
- How does integrate with Airflow work?

Handle high cardinality with explanation 

Handle schema evolution with explanation 

Handle duplication with explanation 

4. Flink Job handling streaming data problems

Base without optimization

- Detail explanation 
- Include Flink UI screen shots

Window processing => capture the code flink handling sliding window in flink

5. Data Storage Optimization

Lakehouse optimization like compaction, z-order and partitioning for each data 

Data Warehouse for example indexing

6. Data Pipeline Orchestration 

DP 1: Raw data into bronze zone => ingest and validate stage 
- Caputre Airflow UI image 

DP 2: Bornze into sliver and then gold zone
- Capture Airflow UI image 

DP 3: Compute offline feature table
- Capture Airflow UI image 

7. Data Governance 

DP1 linked with related tables 
- Linegae between the pipeline and table
- Data validation (which data validation) and contracts (which contracts)

DP2 linked with related tables 

DP3 linked with related tables

8. Documentations 

- Capture DBEAVER screen. 

9. Novel ideas 

- Have 2 ideas 
- Document idea and proof it work 














