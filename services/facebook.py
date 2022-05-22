def extract(connection, cursor, start_date, end_date):
    import requests
    from datetime import timedelta
    import time
    from psycopg2.extras import execute_batch
    from utils import proccess
    from services import postgres

    print(f"> Generating dates from {start_date} to {end_date}")
    dates = [str(start_date+timedelta(days=x)) for x in range((end_date-start_date).days)]
    
    total_record_couter = 0

    token = "TOKEN"
    ad_account_id = 'act_ACCOUNT_ID'
    level = 'ad'
    fields = [
        'ad_name', 
        'ad_id', 
        'campaign_name', 
        'campaign_id', 
        'adset_name', 
        'adset_id', 
        'actions', 
        'spend', 
        'cpc', 
        'cpm', 
        'cpp', 
        'clicks', 
        'reach', 
        'impressions'
        ]
    time_increment = 1
    filtering = []
    date_index = 0
    retries = 0

    updated_columns, custom_fields = postgres.columns_manager(connection, cursor)

    while date_index != (len(dates) - 1):
        
        if total_record_couter > 160:
            time.sleep(3600)

        print(f"> Retrieving data from {dates[date_index]}.")
        data = []
        date_range = {'since': dates[date_index],'until': dates[date_index]}
        url = f"https://graph.facebook.com/v13.0/{ad_account_id}/insights?level={level}&fields={fields}&time_range={date_range}&access_token={token}&time_increment={time_increment}&filtering={filtering}&limit=1000`;"
        
        response = requests.get(url)
        print(f"> Status code: {response.status_code}.")
        if response.status_code == 200:
                
            result = response.json()["data"]


            for record in result:
                record["date"] = record["date_start"]
                handler = proccess.normalize_keys(record, custom_fields)
                row = proccess.record_maker(handler, updated_columns)
                data.append(row)
                total_record_couter +1

            query = """INSERT INTO your_table_here VALUES ({})""".format(','.join('%s' for key in data[0].keys()))
            rows_list = [tuple(x.values()) for x in data]
            
            print(f"> Inserting {len(rows_list)} rows to table")
            
            cursor = connection.cursor()
            
            execute_batch(cursor, query, rows_list)
            connection.commit()
            print("> Data successfully inserted.")
            date_index += 1
            print("> Waiting 15 minutes for next API call.") 
            time.sleep(960)
            
        
        else:
            retries += 1
            print(response.json())

            if response.status_code == 400:
                print(f"> Too many requests for {dates[date_index]}, 1 hour to continue extraction.")
                time.sleep(3600)

            if retries > 5:
                print(f"> Too many retries for {dates[date_index]}")
                break
            pass

        cursor.close()
        connection.close()