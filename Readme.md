## Music streaming app on AWS Platform

A music streaming app company wanted to analyze the data they collected on songs and user activity on their app. The analysis team was particularly interested in understanding what songs users listened to. There was no easy way to query the data to generate the results, since the data resided in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. I created a database and ETL pipeline in both Postgres and Apache Cassandra, designed to optimize queries for understanding what songs users are listening to.

The company grew their user base and song database, and wanted to move the data warehouse onto the cloud. I built an ELT pipeline that extracts their data from S3, stages them in Redshift, and transforms them into a set of dimensional tables for their analytics team.

The company grew their user base and song database even more and wanted to move their data warehouse to a data lake. I built an ELT pipeline that loaded data from S3, processed them into analytics tables using Spark, and load them back into S3.

The company decided to introduce more automation and monitoring to their data warehouse ETL pipelines and came to the conclusion that the best tool to achieve this was Apache Airflow. I created and automated a set of data pipelines. I configured and scheduled data pipelines with Airflow, and then monitored and debugged the production pipelines.