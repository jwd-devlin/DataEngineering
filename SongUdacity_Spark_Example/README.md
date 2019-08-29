# Summary

This project is designed to create a database to store the song and user data for the startup "Sparkify". It is comprised of 3 key files: sql_queries.py, create_tables.py, and elt.py. 


etl.py
    - Extracts, transforms and loads the data into the tables.
        From udacity S3 bucket to another user defined S3.

Requires Modules: spark, pyspark

Create: S3 bucket.
Add: AWS Secret + Key, Name of S3 Bucket.

# How to Run Locally
- Step 0:
    - Run etl.py:   """python etl.py"""

# How to Run EMR
- Step 0:
    - Start EMR cluster with spark.
- Step 1:
    - Open Notebook
    - import etl
    - Run etl

# Questions
- Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
The goal of Sparkify is to set up a database with the purpose of invesigating and understanding what songs users are listening to. 


- State and justify your database schema design and ETL pipeline.
    The database is structure in the star schema design. While this schema suffers from diluted data intergrety it enables simplier queries, making it more user friendly for a start up environment where mulitple people may be required to use the database. 

    Fact Table:
        - Songplays
    Dimension Tables:
        - Songs
        - User
        - Time
        - Artists
- [Optional] Provide example queries and results for song play analysis.
