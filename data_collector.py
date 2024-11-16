import json
import os
import mysql.connector
import paho.mqtt.client as paho
from paho import mqtt

HIVE_USERNAME = os.environ['HIVE_USERNAME']
HIVE_PASSWORD = os.environ['HIVE_PASSWORD']
HIVE_CLUSTER = os.environ['HIVE_CLUSTER']
DB_PASSWORD = os.environ['DB_PASSWORD']

db_config = {
    'user': 'root',
    'password': DB_PASSWORD,
    'host': 'localhost',
    'database': 'RoadSense',
}

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"CONNACK ricevuto con codice {rc}.")

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print(f"Sottoscritto: {mid} con QoS {granted_qos}")

def on_message(client, userdata, message):
    topic_parts = message.topic.split('/')
    username = topic_parts[1]
    dataType = topic_parts[2]
    
    data = json.loads(message.payload.decode())

    if dataType == "pothole":
        save_pothole(username, data)
    elif dataType == "drop":
        save_drop(username, data)
    else:
        print("Messaggio ricevuto, ma non è né un pothole né un drop!")

def save_pothole(username, data):
    user_id = get_user_id(username)

    if user_id is not None:
        cursor, connection = get_db_cursor()
        try:
            cursor.execute("""
                INSERT INTO POTHOLE (userID, latitude, longitude, detection_date)
                VALUES (%s, %s, %s, %s)
            """, (user_id, data['latitude'], data['longitude'], data['detection_date']))
            connection.commit()
            print(f"Pothole salvata: userID={user_id}, lat={data['latitude']}, long={data['longitude']}, date={data['detection_date']}")
        except Exception as e:
            print(f"Errore nel salvataggio della pothole: {e}")
        finally:
            cursor.close()
            connection.close()

def save_drop(username, data):
    user_id = get_user_id(username)

    if user_id is not None:
        cursor, connection = get_db_cursor()
        try:
            cursor.execute("""
                INSERT INTO `DROP` (userID, start_latitude, start_longitude, end_latitude, end_longitude, detection_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, data['start_latitude'], data['start_longitude'], data['end_latitude'], data['end_longitude'], data['detection_date']))
            connection.commit()
            print(f"Drop salvato: userID={user_id}, start_lat={data['start_latitude']}, start_long={data['start_longitude']}, end_lat={data['end_latitude']}, end_long={data['end_longitude']}, date={data['detection_date']}")
        except Exception as e:
            print(f"Errore nel salvataggio del drop: {e}")
        finally:
            cursor.close()
            connection.close()

def get_user_id(username):
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT userID FROM USER WHERE username = %s", (username,))
        result = cursor.fetchone()
        return result[0] if result else None
    finally:
        cursor.close()
        connection.close()

def get_db_cursor():
    connection = mysql.connector.connect(**db_config)
    return connection.cursor(), connection

def main():
    client = paho.Client()
    client.on_message = on_message
    client.on_connect = on_connect
    client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
    client.username_pw_set(HIVE_USERNAME, HIVE_PASSWORD)

    client.connect(HIVE_CLUSTER, 8883)
    client.subscribe("roadsense/#", qos=2)
    client.loop_forever()

if __name__ == "__main__":
    main()
