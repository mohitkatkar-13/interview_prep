🔧 1. Start with the Problem or Use Case
"We needed to build a data pipeline to store and transform the Loans and Deposists data , and make it available for downstream analytics and science teams."

🔄 2. Ingest Layer (Data Sources & Ingestion Tools)
"Data came from multiple sources — mostly Mainframe Or HDFS - For Mainframe we had the MFT and for Unix systems we had SFTP where we used to get the data on Edge node. If the applications are running within teams then we used to directly move the data using hdfs copyToLocal"


Batch: Mainframe Python scripts, Autosys Scheduler, Mainframe CA7 Scheduler

🧪 3. Processing Layer (ETL/ELT & Data Transformation)
Data Quality jobs once the integrity of the Data is done. 
Copy the data to HDFS
processed raw data using PySpark jobs, data cleaning, transformation, and enrichment here."

Batch: PySpark,

🗃️ 4. Storage Layer
Transformed data was stored in Hadoop/Hive as data warehouse partitioned by date.
Also Some of the data was stored in Teradata

Raw/landing: Unix EdgeNode , HDFS

Processed/curated: Hive  , Terdata

🔐 5. Security and Access Control
"Security was achivde using ACL (Access control List) and Kerberos"



CI/CD for pipelines: BitBucket, Jenkins, JFrog Atrifactory 

