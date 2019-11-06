# DataEngineering Airflow:

How to run:

Step 1: Install airflow.

Step 1a: Start redshift cluster.

Step 2: Run Airflow
	- In local directory: export AIRFLOW_HOME="$(pwd)"
	- Start webserver UI: airflow webserver -p 8001
	- Start airflow schedualer* needed or airflow dags wont run: airflow scheduler

Step 3: Create Connections.
	- Create AWS connection with secret keys and region and role.
	- Create redshift connection (DB name, password, port, username, Host locaiton).

Step 4: Run DAG in webserver UI.

