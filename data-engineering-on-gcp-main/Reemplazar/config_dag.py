import os

MYSQL_CONN_ID = os.getenv('MYSQL_CONN_ID', 'Cloud_SQL_db_compass')
PROJECT_ID = os.getenv('PROJECT_ID', 'he-dev-data')
TABLE = os.getenv('TABLE', 'DATA_FLOW_CONFIG')
SA = os.getenv('SA','548349234634-compute@developer.gserviceaccount.com')
