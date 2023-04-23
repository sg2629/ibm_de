# This script
# Extracts data in the file ‘web-server-access-log.txt.gz’ to the table ‘access_log’ in the PostgreSQL database ‘template1’.

# The following are the columns and their data types in the file:

# a. timestamp - TIMESTAMP

# b. latitude - float

# c. longitude - float

# d. visitorid - char(37)

# and two more columns: accessed_from_mobile (boolean) and browser_code (int)

# The columns which we need to copy to the table are the first four coumns : timestamp, latitude, longitude and visitorid.

# Transforms the text delimiter from "#" to ",".
# Loads the data from the TXT file into a table in PostgreSQL database.

# Download the access log file

wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Bash%20Scripting/ETL%20using%20shell%20scripting/web-server-access-log.txt.gz"

# Unzip the file to extract the .txt file.
gunzip -f web-server-access-log.txt.gz

# Extract phase

echo "Extracting data"

# Extract the columns 1 (timestamp), 2 (latitude), 3 (longitude) and 
# 4 (visitorid)
# cut -d"#" -f1-4 web-server-access-log.txt

# Redirect the extracted output into a file.

cut -d"#" -f1-4 web-server-access-log.txt  > extracted-data.txt

# Transform phase
echo "Transforming data"
# read the extracted data and replace the # with commas.

# tr "#" "," < extracted-data.txt

tr "#" "," < extracted-data.txt > transformed-data.csv


# Load phase
echo "Loading data"

# Send the instructions to connect to 'template1' and
# copy the file to the table 'access_log' through command pipeline.

echo "\c template1;\COPY access_log  FROM '/home/project/transformed-data.csv' DELIMITERS ',' CSV HEADER;" | psql --username=postgres --host=localhost