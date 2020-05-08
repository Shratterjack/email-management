from dotenv import load_dotenv
import mysql.connector
import os

load_dotenv('localhost.env')

# creates connection by taking parameters from env file 
# to mysql database and returns it
def getDBConnection():
    mydb = mysql.connector.connect(
    host=os.getenv("host"),
    user=os.getenv("user"),
    password=os.getenv("password"),
    database=os.getenv("database"),
    )
    return mydb
