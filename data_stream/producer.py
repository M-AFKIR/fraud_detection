from confluent_kafka import Producer
import json
import uuid
import os
from dotenv import load_dotenv
import pandas as pd 
import time 
import random




def delivery_report(err, msg):
    if err:
        print(f"delivery failed : {err}")
    else:
        print(f"message delivered : {msg.value().decode('utf-8')}")



def main():
    load_dotenv()

    records = pd.read_csv('data/test.csv').to_dict(orient='records')

    
    producer_config = {
        'bootstrap.servers': os.getenv("BOOTSTRAP_SERVER"),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv("CONFLUENT_API_KEY"),
        'sasl.password': os.getenv("CONFLUENT_API_SECRET"),
        'broker.address.family': 'v4'
    } 

    producer = Producer(producer_config)
    topic_name = os.getenv("TXC_TOPIC")

    try :

        while True:

            try:
                
                row = random.choice(records).copy()
                row['event_timestamp'] = time.time()


                payload = {
                    "id": uuid.uuid4().hex,
                    "data": row
                }

                producer.produce(
                    topic=topic_name,
                    key= str(row['id']).encode('utf-8'),
                    value= json.dumps(payload).encode('utf-8'),
                    callback=delivery_report
                )

                delay = random.uniform(0.01, 1.5)

                time.sleep(delay)

                producer.poll(0)

            except Exception as e:
                print(f"Unexpected error in loop: {e}")
                time.sleep(1)
                
    finally :
        print("\nFlushing remaining messages...")
        producer.flush()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n Producer manually stopped. Shutting down cleanly.")