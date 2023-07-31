"""
Purpose of Script:
    This script retrieves payment information found in the Data Warehouse and delivers it to Accounting found in a target folder. 
    Once the list of payments are retrieved, filters and identifies the most recent ones based on the last date found in the target folder and the next business day.
    Once the list has been filtered down, cycle through each unique payment id and extracts a csv with all the payment recrods pertaining to the payment.
    This script is built to be used on-demand by non-technical users so print statements show the user the progress on the script and allow for quick error handling. 

"""
from datetime import datetime as dt, timedelta
import holidays
import time
import os

from pathlib import Path
import pyodbc
import csv

# Calculates Today's Date & Formats into "YYYYMMDD"
if dt.now().month < 10:
    Month = '0'+str(dt.now().month)
else:
    Month = str(dt.now().month)

if dt.now().day < 10:
    Day = '0'+str(dt.now().day)
else:
    Day = str(dt.now().day)

Year = str(dt.now().year)

Today = Year+Month+Day

# File path and name.
filePath = r"\\SharedFolder\TargetLocation"
fileName = 'Payment_to_Accounting_'+Today
fileNameEnding = '.csv'

# Retrieves USA holiday schedule
ONE_DAY = timedelta(days=1)
HOLIDAYS_US = holidays.US()

# Retrieves the next business day
def next_business_day():
    next_day = dt.today() + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day += ONE_DAY
    return next_day

# Formats next b day "YYYYMMDD"
if next_business_day().month < 10:
    BMonth = '0'+str(next_business_day().month)
else:
    BMonth = str(next_business_day().month)

if next_business_day().day < 10:
    BDay = '0'+str(next_business_day().day)
else:
    BDay = str(next_business_day().day)
BYear = str(next_business_day().year)
NextBDay = BYear+BMonth+BDay
latestFileMax = NextBDay

# Get last extract date
listOfFiles = os.listdir(filePath)
latestFile = max(listOfFiles)

# Database connection variable.
connect = None

# Check if the file path exists.
if os.path.exists(filePath):

    try:

        # Connect to database.
        print("Connecting to Database")

        connect = pyodbc.connect('Driver={SQL Server};'  # Database Application
                                 'Server=WarehouseServer;'  # Data warehouse where we host our data
                                 'Database=Database;'  # Read-Only access to view the data
                                 'Trusted_Connection=yes;')
        time.sleep(5)
        print("Connection Successful")

    except pyodbc.Error as e:

        # Confirm unsuccessful connection and stop program execution.
        print("Database connection unsuccessful.")
        time.sleep(5)
        quit()

    # Cursor to execute query.
    cursor = connect.cursor()

    # Get distinct list of Payment Wires
    print("Identifying new payments...")
    WireListSQL = []
    WireListSQL = \
        """SELECT DISTINCT PAYMENT_REF 
        FROM PaymentTable P 
        WHERE 1=1
        AND TRANSFER_DATE >  """+latestFile+"""
        AND TRANSFER_DATE <  """+latestFileMax+"""

        GROUP BY PAYMENT_REF 
        HAVING SUM(TOTAL_PAYMENT_AMT) > 0"""

    # Execute query.
    cursor.execute(WireListSQL)

    # Fetch the data returned.
    NumberOfWires = [i[0] for i in cursor.fetchall()]
    if not NumberOfWires:
        print("No new payments found")
        time.sleep(10)
        quit()
        
print("Number of Files: " + str(len(NumberOfWires)))
for i in range(len(NumberOfWires)):
    # SQL to select data from the person table.
    sqlSelect = \
        """SELECT * 
           FROM PaymentTable P
           WHERE PAYMENT_REF = """ + str(NumberOfWires[i]) + " AND TRANSFER_DATE >= '"+latestFile+"'"

    try:

        # Execute query.
        cursor.execute(sqlSelect)

        # Fetch the data returned.
        results = cursor.fetchall()

        # Extract the table headers.
        headers = [i[0] for i in cursor.description]

        # Creates File Folder if it does not exist
        if not os.path.exists(filePath + '\\' + Today):
            os.makedirs(filePath + '\\' + Today)

        # Open CSV file for writing.
        with open(filePath + '\\' + Today + '\\' + fileName + '_' + str(NumberOfWires[i]) + fileNameEnding, 'w') as csvFile:

            # Create CSV writer.
            writer = csv.writer(csvFile, delimiter=',', lineterminator='\r',
                                quoting=csv.QUOTE_NONE, escapechar='\\')

            # Add the headers and data to the CSV file.
            writer.writerow(headers)
            writer.writerows(results)

        # Message stating export successful.
        print("Data export successful.")

    except pyodbc.Error as e:

        # Message stating export unsuccessful.
        print("Data export unsuccessful.")
        quit()

# Close database connection.
connect.close()
print("All files generated")
time.sleep(5)
