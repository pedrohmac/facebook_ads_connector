def get_connection():
    import psycopg2
    from services import secrets

    print("Connecting to database.")
    conn = psycopg2.connect(
        host=secrets.credentials['host'],
        database=secrets.credentials['database'],
        user=secrets.credentials['user'],
        password=secrets.credentials['password']
    )
    cur = conn.cursor()

    return conn, cur

def get_custom_fields():
    ## returns dict of name : id on custom fields
    import requests
    token = 'YOUR_TOKEN'
    url = f"https://graph.facebook.com/v13.0/act_YOUR_ACCOUNT_HERE/customconversions?fields=name&access_token={token}"
    response = requests.get(url)
    custom_fields = {i["name"]: i["id"] for i in response.json()["data"]}
    return custom_fields


def create_columns(connection, cursor, missing_columns):
    ## creates newly identified custom fields columns
    for column in missing_columns:
        cursor.execute(f"""ALTER TABLE your_table_here ADD COLUMN "{column}" int null;""")
        connection.commit()
    created_columns = ", ".join(missing_columns)
    return print(f"> Columns {created_columns} were successfully created.")


def columns_manager(connection, cursor):
    cursor.execute("""SELECT * FROM your_table_here LIMIT 0""")
    cursor.fetchone()
    columns = [col[0] for col in cursor.description]

    custom_fields = get_custom_fields()

    missing_columns = []

    for field in custom_fields.keys():
        if field not in columns:
            missing_columns.append(field)

    if len(missing_columns) > 0:
        create_columns(connection, cursor, missing_columns)
    
    else:
        print("> No columns to create.")

    cursor.execute("""SELECT * FROM your_table_here LIMIT 0""")
    cursor.fetchone()
    updated_columns = [col[0] for col in cursor.description]

    cursor.close()
    connection.close()

    return updated_columns, custom_fields

