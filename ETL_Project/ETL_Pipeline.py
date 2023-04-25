# wget https://archive.apache.org/dist/kafka/2.8.0/kafka_2.12-2.8.0.tgz
# tar -xzf kafka_2.12-2.8.0.tgz
# start_mysql
# mysql --host=127.0.0.1 --port=3306 --user=root --password=MjI4Nzgtc2dhbzA3
# create database tolldata;
# use tolldata;

# create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);
# exit
# python3 -m pip install kafka-python
# python3 -m pip install mysql-connector-python==8.0.31
# cd kafka_2.12-2.8.0
# bin/zookeeper-server-start.sh config/zookeeper.properties
# cd kafka_2.12-2.8.0
# bin/kafka-server-start.sh config/server.properties

# create a topic
# cd kafka_2.12-2.8.0
# bin/kafka-topics.sh --create --topic toll --bootstrap-server localhost:9092

# wget 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/toll_traffic_generator.py'

# Configure streaming_data_reader.py
# wget 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/streaming_data_reader.py'

#streaming data reader
"""
Streaming data consumer
"""
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector

TOPIC='toll'
DATABASE = 'tolldata'
USERNAME = 'root'
PASSWORD = 'MjI4Nzgtc2dhbzA3'

print("Connecting to the database")
try:
    connection = mysql.connector.connect(host='localhost', database=DATABASE, user=USERNAME, password=PASSWORD)
except Exception:
    print("Could not connect to database. Please check credentials")
else:
    print("Connected to database")
cursor = connection.cursor()

print("Connecting to Kafka")
consumer = KafkaConsumer(TOPIC)
print("Connected to Kafka")
print(f"Reading messages from the topic {TOPIC}")
for msg in consumer:

    # Extract information from kafka

    message = msg.value.decode("utf-8")

    # Transform the date format to suit the database schema
    (timestamp, vehcile_id, vehicle_type, plaza_id) = message.split(",")

    dateobj = datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')
    timestamp = dateobj.strftime("%Y-%m-%d %H:%M:%S")

    # Loading data into the database table

    sql = "insert into livetolldata values(%s,%s,%s,%s)"
    result = cursor.execute(sql, (timestamp, vehcile_id, vehicle_type, plaza_id))
    print(f"A {vehicle_type} was inserted into the database")
    connection.commit()
connection.close()
