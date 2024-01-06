import boto3
import json
import os


# Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("postcodes").master("local[*]").config("spark.jars", "postgresql-42.5.0.jar").getOrCreate()

localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT", "localhost")
postgres_host = os.environ.get("POSTGRES_HOST", "localhost")
postgres_port = os.environ.get("POSTGRES_PORT", "5432")
postgres_user = os.environ.get("POSTGRES_USER", "admin")
postgres_password = os.environ.get("POSTGRES_PASSWORD", "admin")
postgres_db = os.environ.get("POSTGRES_DB", "bia")

def send_message(message):
    sqs_client = boto3.client("sqs", endpoint_url=f"http://{localstack_endpoint}:4566")

    response = sqs_client.send_message(
        QueueUrl=f"http://{localstack_endpoint}:4566/000000000000/coordinates",
        MessageBody=json.dumps(message)
    )
    print(response)

def process_partition(iterator):
    geolocations = []
    for row in iterator:
        geolocations.append({
            "latitude": row.lat,
            "longitude": row.lon,
            "limit": 1
        })

    payload = {
        "geolocations": geolocations
    }

    send_message(payload)

def get_post_code(df):
    batch_size = 100
    df_repartition = df.repartition(df.count() // batch_size + 1)
    df_repartition.foreachPartition(process_partition)

def get_df_coordinates():
    db_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    db_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver",
    }

    return spark.read.jdbc(url=db_url, table='coordinates', properties=db_properties)

def delete_message(receipt_handle):
    print(f"Deleting message with receipt handle: {receipt_handle}")
    sqs_client = boto3.client("sqs", endpoint_url=f"http://{localstack_endpoint}:4566")
    response = sqs_client.delete_message(
        QueueUrl=f"http://{localstack_endpoint}:4566/000000000000/process",
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
                QueueUrl=f"http://{localstack_endpoint}:4566/000000000000/process",
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
            )

            print(f"Number of messages received: {len(response.get('Messages', []))}")
            print(f"Number of messages received total: {count}")

            for message in response.get("Messages", []):
                count += 1

                message_body = message["Body"]
                print(f"Message body: {json.loads(message_body)}")

                receipt_handle = message['ReceiptHandle']
                print(f"Receipt Handle: {receipt_handle}")

                try:
                    df_coordinates = get_df_coordinates()
                    get_post_code(df_coordinates)
                except Exception as e:
                    print(e)

                delete_message(receipt_handle)
                
        except Exception as e:
            print(e)


def main():
    receive_message()

main()