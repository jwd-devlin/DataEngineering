# Summary
The goal of this data pipeline was to set a data-warehouse for data around US cities. With a particular around
city population and the yearly immigration reports released. This warehouse is to maintain as a source of truth for further
study by data analysts.

One example question to be answered from this data-warehouse is to see the ratio of immigrants arriving in near by
airports in relation to their recorded population.

"""
What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project."""

Data:
    - City Population data:
    - US Immigration data for 2016.
    - City coordinates data.
    - US Airport information.

For data cleaning see the respective data cleaning functions within each data source`s data_cleaning function.


# About the data


# Need To Know
Requires Modules: Pandas, Airflow

# How to Run
Step 0: Setup data (Local or S3)
      - add paths to data data_locations.py in helpers folder.

Step 1: Install airflow.

Step 1a: Start redshift cluster.

Step 2: Run Airflow
	- In local directory: export AIRFLOW_HOME="$(pwd)"
	- Start webserver UI: airflow webserver -p 8001
	- Start airflow scheduler (* needed or airflow dags wont run: airflow scheduler)

Step 3: Create Connections.
	- Create AWS connection with secret keys and region and role.
	- Create redshift connection (DB name, password, port, username, Host location).

Step 4: File setup.
     - Edit dag operators depending on where data is stored (S3 or local)


Step 5: Run DAG in webserver UI.


## Hypothetical Questions:
If the data was increased by 100x.
    - Accessing data local would be the first and most obvious problem, data storage would have to be stored remotely
    (example: s3 buckets). Accessing the some of source files will have to be reconfigured to retrieve files in chunks.
    Monitoring of cloud resources would be required to manage the financial costs of the data increase.
If the pipelines were run on a daily basis by 7am.
    - Setup airflow to operate in the cloud. Set a trigger to start at an earlier time with buffer to reset and start again.
    With a second/third failure an alarm should be set to alert if the 7am will not be reached.
If the database needed to be accessed by 100+ people.
    -Set up role structure for users. Create isolated backups, from general users. Allow set teams to access different
    instances of the data, to allow for balancing of loads.