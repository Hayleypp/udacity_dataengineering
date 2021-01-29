# Datalake

This project provides ideas to an music startup, Sparkify. As their data has grown more, they want to move their data warehouse to a datalake. They can also analyse their service and figure out who listened which song and when.

------

Data is in S3, in a directory of JSON logs on user activity, a directory of JSON metadata on the songs in an app as well. To build an ETL pipeline that extracts data from S3, and it is processed through Spark (Spark process on a cluster using AWS), and then it is loaded back to S3. 