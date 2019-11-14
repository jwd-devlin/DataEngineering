# Summary
The goal of this data pipeline was to set a data-warehouse for data around US cities. With a particular around
city population.

One example question to be answered from this data-warehouse is to see the ratio of immigrants arriving in near by
airports in relation to their recorded population.

Data:
    - City Population data:
    - US Immigration data for 2016.
    - City coordinates data.
    - US Airport information.


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


