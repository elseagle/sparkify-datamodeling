## About
This project focuses on an ETL pipeline to process music data using Postgres DB and python

### Fact Table 
- `songplays` - records in log data associated with song plays i.e. records with page NextSong
`songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Dimension Tables
- `users` - users in the app: `user_id`, `first_name`, `last_name`, `gender`, `level`
- `songs` - songs in music database : `song_id`, `title`, `artist_id`, `year`, `duration`
- `artists` - artists in music database : `artist_id`, `name`, `location`, `latitude`, `longitude`
- `time` - timestamps of records in songplays broken down into specific units : `start_time`, `hour`, `day`, `week`, `month`, 
  `year`, `weekday`


## Setup Application
- To create the tables run: \
``python3 create_tables.py``
- To confirm the tables have been created run the cells in `test.ipynb`
- To perform the etl run: \
  ``python3 etl.py``
- To confirm the etl was successful run the cells in `test.ipynb`

## Files in the repository
- `create_tables` - creates the database and tables needed 
- `etl.ipnyb` - jupyter notebook for experimentation
- `etl.py` - processing of the Extraction, Transformation and Loading pipeline
- `sql_queries.py` - The query strings needed by create_tables
- `test.ipynb` - test environment to confirm tables are seeded

*N.B: Creds in scripts are local host creds, hence .env or .cfg config files were not used*