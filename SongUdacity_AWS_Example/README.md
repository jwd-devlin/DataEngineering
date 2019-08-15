# Summary

This project is designed to create a database to store the song and user data for the startup "Sparkify". It is comprised of 3 key files: sql_queries.py, create_tables.py, and elt.py. 

sql_queries.py
    - Holds all the sql queries for creation of tables, inserts, and extraction. 
create_tables.py
    - Access the database and initalises the tables.
etl.py
    - Extracts, transforms and loads the data into the tables.

Requires Modules: Pandas, psycopg2

# How to Run
- Step 0: * Within the jupyter
    - Setup the AWS: user and role
    - Add additional information into the config file.
    - Build redshift database.
- Step 1:
    - Run create_tables.py:   """python create_tables.py"""
- Step 2:
    - Run etl.py:   """python etl.py"""



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
