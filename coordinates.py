import boto3
import json
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, Row
import psycopg2
import os

spark = SparkSession.builder.appName("coordinates").master("local[*]").config("spark.jars", "postgresql-42.5.0.jar").getOrCreate()

schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("postcode", StringType(), True),
])

localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "localhost")
postgres_host = os.environ.get("POSTGRES_HOST", "localhost")
postgres_port = os.environ.get("POSTGRES_PORT", "5432")
postgres_user = os.environ.get("POSTGRES_USER", "admin")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "admin")
postgres_db = os.environ.get("POSTGRES_DB", "bia")

def update_postcode(row):
    sql = """UPDATE coordinates SET postcode = %s WHERE lat = %s and lon = %s"""
    conn = None

    try:
        conn = psycopg2.connect(database=postgres_db,user=postgres_user,password=postgres_password,host=postgres_host,port=postgres_port)
        cursor = conn.cursor()
        cursor.execute(sql, (row.postcode, row.lat, row.lon))
        cursor.close()
        conn.commit()
        conn.close()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_coordinates(df):
    db_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    db_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    df.write.jdbc(url=db_url, table='coordinates', mode='append', properties=db_properties)

def consume_postcodes_service(payload):
    response = requests.post("https://api.postcodes.io/postcodes?filter=postcode", json=payload)
    if not response.status_code == 200:
         print(f"Error en la solicitud: {response.status_code} - {response}")
         return None
    
    return response.json()

def convert_response_service_to_df(response_data):
    results = response_data.get("result", [])
    rows = []

    for result in results:
        query = result.get("query", {})
        lon = float(query.get("longitude"))
        lat = float(query.get("latitude"))

        result_postcode = result.get("result")
        
        postcode = None
        if result_postcode:
            postcode = result_postcode[0].get("postcode")

        rows.append(Row(lat=lat, lon=lon, postcode=postcode))
        
    return spark.createDataFrame(rows, schema=schema)

def delete_message(receipt_handle):
    print(f"Deleting message with receipt handle: {receipt_handle}")
    sqs_client = boto3.client("sqs", endpoint_url=f"http://{localstack_endpoint}:4566")
    response = sqs_client.delete_message(
        QueueUrl=f"http://{localstack_endpoint}:4566/000000000000/coordinates",
        ReceiptHandle=receipt_handle,
    )
    print(response)


def receive_message():
    count = 0

    while True:
        try:
            print("Waiting for messages...")
            sqs_client = boto3.client("sqs", endpoint_url=f"http://{localstack_endpoint}:4566")
            response = sqs_client.receive_message(
                QueueUrl=f"http://{localstack_endpoint}:4566/000000000000/coordinates",
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )

            print(f"Number of messages received: {len(response.get('Messages', []))}")
            print(f"Number of messages received total: {count}")

            for message in response.get("Messages", []):
                count += 1

                message_body = message["Body"]
                payload = json.loads(message_body)
                
                print(f"Message body: {payload}")

                receipt_handle = message['ReceiptHandle']
                print(f"Receipt Handle: {receipt_handle}")

                # Main Process
                reponse = consume_postcodes_service(payload)
                df = convert_response_service_to_df(reponse)
                df.foreach(update_postcode)


                delete_message(receipt_handle)
                
        except Exception as e:
            print(e)


def main():
    receive_message()

main()