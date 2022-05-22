
import datetime
from services import facebook, postgres

start_date = datetime.datetime(2022,1,12).date()
end_date = datetime.datetime(2022,4,14).date()

connection, cursor = postgres.get_connection()

facebook.extract(connection, cursor, start_date, end_date)
