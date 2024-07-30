# config.py

import os

# Set environment variables for database connection
os.environ['INSTANCE_CONNECTION_NAME'] = 'he-dev-compass'
os.environ['DB_USER'] = 'compass_admin'
os.environ['DB_PASSWORD_SECRET_ID'] = 'csql_compass'
os.environ['DB_NAME'] = 'db-compass'
os.environ['GCP_PROJECT'] = 'he-dev-data'
os.environ['HOST'] = '35.245.223.109'
os.environ['PORT'] = '3306'

PREFIX = os.getenv('PREFIX', '')  # Set your prefix if needed
WEB_SERVER_URL = os.getenv('WEB_SERVER_URL', 'https://80bb830408f34d3ca939b9b8d3524076-dot-us-east4.composer.googleusercontent.com')
