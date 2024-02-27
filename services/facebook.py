import requests
from datetime import timedelta
import time
from psycopg2.extras import execute_batch
import logging
from utils import process
from services import postgres

logging.basicConfig(level=logging.INFO)

def generate_date_range(start_date, end_date):
    logging.info(f"Generating dates from {start_date} to {end_date}")
    return [str(start_date + timedelta(days=x)) for x in range((end_date - start_date).days)]

def retrieve_data(connection, cursor, date, token, ad_account_id, level, fields, time_increment, filtering, retries, updated_columns, custom_fields):
    total_record_counter = 0

    if total_record_counter > 160:
        time.sleep(3600)

    logging.info(f"Retrieving data from {date}.")
    data = []
    date_range = {'since': date, 'until': date}
    url = f"https://graph.facebook.com/v13.0/{ad_account_id}/insights?level={level}&fields={','.join(fields)}&time_range={date_range}&access_token={token}&time_increment={time_increment}&filtering={filtering}&limit=1000"
    
    response = requests.get(url)
    logging.info(f"Status code: {response.status_code}.")

    if response.status_code == 200:
        result = response.json()["data"]

        for record in result:
            record["date"] = record["date_start"]
            handler = process.normalize_keys(record, custom_fields)
            row = process.record_maker(handler, updated_columns)
            data.append(row)
            total_record_counter += 1

        query = """INSERT INTO your_table_here VALUES ({})""".format(','.join('%s' for key in data[0].keys()))
        rows_list = [tuple(x.values()) for x in data]
        
        logging.info(f"Inserting {len(rows_list)} rows to table")
        
        cursor = connection.cursor()
        execute_batch(cursor, query, rows_list)
        connection.commit()
        logging.info("Data successfully inserted.")
        logging.info("Waiting 15 minutes for next API call.") 
        time.sleep(960)
        
    else:
        retries += 1
        logging.error(response.json())

        if response.status_code == 400:
            logging.warning(f"Too many requests for {date}, 1 hour to continue extraction.")
            time.sleep(3600)

        if retries > 5:
            logging.error(f"Too many retries for {date}")
            return False

    return True

def extract(connection, cursor, start_date, end_date):
    token = "TOKEN"
    ad_account_id = 'act_ACCOUNT_ID'
    level = 'ad'
    fields = [
        'ad_name', 'ad_id', 'campaign_name', 'campaign_id',
        'adset_name', 'adset_id', 'actions', 'spend', 'cpc',
        'cpm', 'cpp', 'clicks', 'reach', 'impressions'
    ]
    time_increment = 1
    filtering = []
    retries = 0

    updated_columns, custom_fields = postgres.columns_manager(connection, cursor)

    dates = generate_date_range(start_date, end_date)
    
    for date in dates:
        success = retrieve_data(connection, cursor, date, token, ad_account_id, level, fields, time_increment, filtering, retries, updated_columns, custom_fields)
        if not success:
            break

    cursor.close()
    connection.close()
