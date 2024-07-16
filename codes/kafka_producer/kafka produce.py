from confluent_kafka import Producer
import json
import time





# Configuration for the Kafka Producer
producer_config = {
    'bootstrap.servers': f'pkc-56d1g.eastus.azure.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'ZF2Q6AFNI4D523WD',
    'sasl.password': "CpHTEEkE1W7F3kl/Q2w67TOkterNjVXDJUNZymx++bOBpiWZMatK1vw3eBI9OioI",
    'client.id': "varun-laptop"
}

topic = "invoice_1"

def delivery_callback(err, msg):
    key = msg.key().decode('utf-8')
    invoice_id = json.loads(msg.value().decode('utf-8'))["InvoiceNumber"]

    if err is not None:
        print('Delivery failed for message {}'.format(err))
    else:
        print(f"Message delivered to: key= {key} value={invoice_id}")

def produce_invoices(producer, count):
    counter = 0
    with open("data/Single_Line_Invoices_4.json") as file:
        for line in file:
            invoice_json_object = json.loads(line)
            store_id = invoice_json_object["StoreID"]
            producer.produce(topic,
                             key=store_id,
                             value=line,
                             callback=delivery_callback)
            time.sleep(0.5)
            producer.poll(1)
            counter += 1
            if counter == count:
                break

def start_producing_invoices(count):
    kafka_producer = Producer(producer_config)
    produce_invoices(kafka_producer, count)
    kafka_producer.flush(10)

# Call this function with the number of invoices you want to produce

start_producing_invoices(80)