import psycopg2
import requests
import logging
from services import secrets

logging.basicConfig(level=logging.INFO)

def get_connection():
    logging.info("Connecting to database.")
    conn = psycopg2.connect(
        host=secrets.credentials['host'],
        database=secrets.credentials['database'],
        user=secrets.credentials['user'],
        password=secrets.credentials['password']
    )
    cur = conn.cursor()
    return conn, cur

def get_custom_fields():
    logging.info("Fetching custom fields from Facebook.")
    token = 'YOUR_TOKEN'
    url = f"https://graph.facebook.com/v13.0/act_YOUR_ACCOUNT_HERE/customconversions?fields=name&access_token={token}"
    response = requests.get(url)
    custom_fields = {i["name"]: i["id"] for i in response.json()["data"]}
    return custom_fields

def create_columns(connection, cursor, missing_columns):
    for column in missing_columns:
        cursor.execute(f"""ALTER TABLE your_table_here ADD COLUMN "{column}" int null;""")
        connection.commit()
    created_columns = ", ".join(missing_columns)
    logging.info(f"Columns {created_columns} were successfully created.")

def columns_manager(connection, cursor):
    logging.info("Managing columns...")
    cursor.execute("""SELECT * FROM your_table_here LIMIT 0""")
    cursor.fetchone()
    columns = [col[0] for col in cursor.description]

    custom_fields = get_custom_fields()

    missing_columns = [field for field in custom_fields.keys() if field not in columns]

    if missing_columns:
        create_columns(connection, cursor, missing_columns)
    else:
        logging.info("No columns to create.")

    cursor.execute("""SELECT * FROM your_table_here LIMIT 0""")
    cursor.fetchone()
    updated_columns = [col[0] for col in cursor.description]

    cursor.close()
    connection.close()

    return updated_columns, custom_fields
