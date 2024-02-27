import datetime
import argparse
import json
from services import facebook, postgres

def parse_date(date_str):
    try:
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Invalid date format. Please use YYYY-MM-DD.")

def handler(event, context):
    if event.get('start_date') and event.get('end_date'):
        start_date = parse_date(event['start_date'])
        end_date = parse_date(event['end_date'])
    else:
        # Fallback to default dates or handle accordingly
        start_date = datetime.datetime(2022, 1, 12).date()
        end_date = datetime.datetime(2022, 4, 14).date()

    connection, cursor = postgres.get_connection()

    facebook.extract(connection, cursor, start_date, end_date)

    return {
        'statusCode': 200,
        'body': json.dumps('Extraction completed successfully!')
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract data from Facebook.")
    parser.add_argument("--start_date", type=parse_date, help="Start date in the format YYYY-MM-DD")
    parser.add_argument("--end_date", type=parse_date, help="End date in the format YYYY-MM-DD")
    args = parser.parse_args()

    connection, cursor = postgres.get_connection()

    facebook.extract(connection, cursor, args.start_date, args.end_date)
