
import paho.mqtt.client as mqtt
import json
from mysql.connector import MySQLConnection, Error, errorcode
from datetime import datetime
import os, time
from pytz import timezone


fmt = '%Y-%m-%d %H:%M:%S'
# define your timezone mine is asia kathmandu
eastern = timezone('Asia/Kathmandu')
naive_dt = datetime.now()
print(naive_dt.strftime(fmt))

#create a error file to save your error log
logFile = r'log/errorfile.txt'
with open(logFile, 'w') as f:
    f.write('File created\n')

#database connecting code
try:
    conn = mysql.connector.connect(host='localhost',
                                           database='tempHumGraph',
                                           user='root',
                                           password='Smart@135'
                                           )
    if conn.is_connected():
        print('Connected to MySQL database')
    else:
        print('Not connected to MySQL database')
except Error as e:
    print(e)
    with open(logFile, 'a') as f:
        loc_dt = datetime.now(eastern)
        print("Error = {} \n Time = {} \n".format(e, loc_dt.strftime(fmt)), file = f)
        

def query_with_fetchall(query):
    try:
        #dbconfig = read_db_config()
        #conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor(buffered = True)
        cursor.execute(query)
        rows = cursor.fetchall()
        
 
        print('Total Row(s):', cursor.rowcount)
        cursor.close()
        return rows
 
    except Error as e:
        print(e)
        with open(logFile, 'a') as f:
            loc_dt = datetime.now(eastern)
            print("Error = {} \n Time = {} \n".format(e, loc_dt.strftime(fmt)), file = f)
        
def query_fetchone(query, topic):
    try:
        cursor = conn.cursor(buffered = True)
        cursor.execute(query, (topic,))
        record = cursor.fetchone()
        print(record[0])
        cursor.close()
        return record[0]
    except Error as e:
        print(e)
        with open(logFile, 'a') as f:
            loc_dt = datetime.now(eastern)
            print("Error = {} \n Time = {} \n".format(e, loc_dt.strftime(fmt)), file = f)

 
def insert_temphum(temp, hum, did):
    try:
        loc_dt = datetime.now(eastern)
        print(loc_dt.strftime(fmt))
        #you need to write a query according to your database table mine is like
        mySql_insert_query = """INSERT INTO datatemphum (temp,humidity, deviceId, cDate) 
                               VALUES(%s, %s, %s, %s) """
        recordTuple = (temp, hum, did, int(time.time()))
        cursor = conn.cursor(buffered = True)
        cursor.execute(mySql_insert_query, recordTuple)
        conn.commit()
        print("Record inserted successfully into table")
        cursor.close()
        
    except mysql.connector.Error as error:
        print("Failed to insert record into table {}".format(error))
        with open(logFile, 'a') as f:
            loc_dt = datetime.now(eastern)
            print("Error = {} \n Time = {} \n".format(error, loc_dt.strftime(fmt)), file = f)
    

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    rows = query_with_fetchall("select id, mqttTopic from iotdevice")
    for row in rows:
        print(row[1])
        client.subscribe(row[1])

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
   

def insertChar(mystring, position, chartoinsert ):
    longi = len(mystring)
    mystring   =  mystring[:position] + chartoinsert + mystring[position:] 
    return mystring 


# The callback for when a PUBLISH message is received from the server.

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    mqtt_payload = json.loads(msg.payload)
    temp = str(mqtt_payload['env_temp'])
    temp_with_dot = (insertChar(temp,2, '.'))
    
    humidity = str(mqtt_payload['humidity'])
    humi_with_dot = (insertChar(humidity,2, '.'))
    print(temp_with_dot)
    print(humi_with_dot)
    topic = msg.topic
    print(topic)
    device_id = query_fetchone("select id from iotdevice where mqttTopic = %s", topic)
    
    insert_temphum(temp_with_dot, humi_with_dot, device_id)
    
    #parse adversting data

broker_address= "1.1.1.1"  #Broker ip address
port = 1883                         #Broker port
user = "yourusername"                    #Connection username
password = "yourpassword"            #Connection password


client = mqtt.Client()
client.username_pw_set(user, password)    #set username and password
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
 
