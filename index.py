from quickstart import buildConnection
from database import getDBConnection
from mail import Mail
import time


start_time = time.time()
print("Start Time:" ,start_time)
# google api client connection
service = buildConnection()

# database client connection
db = getDBConnection()

mailObj = Mail(service, db)
messages = mailObj.executeRequests()
end_time = time.time()
print("End Time:", end_time)
print("Time taken to ingest data:", end_time-start_time)
