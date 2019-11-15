class SqlQueries:
    # CREATE TABLES

    us_states_table = """
    CREATE TABLE IF NOT EXISTS states (
	    ID INTEGER PRIMARY KEY SORTKEY,
	    state_code VARCHAR NOT NULL,
	    state VARCHAR NOT NULL);
    """
    us_city_table = """
    CREATE TABLE IF NOT EXISTS cities (
	    ID INTEGER PRIMARY KEY SORTKEY,
	    ID_STATE INTEGER NOT NULL FOREIGN KEY,
	    city VARCHAR NOT NULL,
	    county VARCHAR NOT NULL,
	    LATITUDE DOUBLE PRECISION NOT NULL,
	    LONGITUDE DOUBLE PRECISION NOT NULL);
    """

    us_immigration_table = """
CREATE TABLE IF NOT EXISTS immigration (
	ID INTEGER IDENTITY(1,1) PRIMARY KEY SORTKEY,
	i94port VARCHAR,
	biryear INT,
	i94cit VARCHAR,
	depdate VARCHAR,
	i94visa VARCHAR,
	i94mon INT,
	i94yr INT
);
    
    """

    us_city_demographics = """
CREATE TABLE IF NOT EXISTS demographics (
	ID INT PRIMARY KEY,
    city	VARCHAR NOT NULL,
    State	VARCHAR NOT NULL,
    median_age	VARCHAR,
    male_population	FLOAT ,
    female_population	FLOAT ,
    total_population	FLOAT,
    foreign_born	FLOAT ,
    average_household_size	FLOAT ,
    state_code	VARCHAR NOT NULL);
    """

    us_airports = """
CREATE TABLE  IF NOT EXISTS airports (
	ID INT  PRIMARY KEY,
	ident	VARCHAR NOT NULL,
	name	VARCHAR NOT NULL,
	elevation_ft	VARCHAR NOT NULL,
	iso_region	VARCHAR NOT NULL,
	municipality	VARCHAR NOT NULL,
	coordinates	VARCHAR NOT NULL,
	LATITUDE	DOUBLE PRECISION NOT NULL,
	LONGITUDE  DOUBLE PRECISION NOT NULL);
    """


