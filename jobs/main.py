import json
import os
import random
import time
from confluent_kafka import SerializingProducer
import requests

API_KEY = 'agqoq5PESTYHZ1rP0TX4tFt7pHKu5pjC'
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
EVENT_TOPIC = os.getenv('EVENT_TOPIC','event_data')
SALES_TOPIC = os.getenv('SALES_TOPIC','sales_data')
CLASSIFICATION_TOPIC = os.getenv('CLASSIFICATION_TOPIC','classification_data')
VENUE_TOPIC = os.getenv('VENUE_TOPIC','venue_data')
ATTRACTION_TOPIC = os.getenv('ATTRACTION_TOPIC','attraction_data')

def get_event_info(event):
    return {'id': event['id'],
            'event_name': event['name'],
            'link': event['url'],
            'locale': event['locale'],
            'event_date': event['dates']['start']['localDate']}
def get_sales_data(event):
    return {'id': event['id'],
            'Start_SalesDate': event['sales']['public']['startDateTime'],
            'End_SalesDate': event['sales']['public']['endDateTime'],
            'minPrice': event.get('priceRanges', [{}])[0].get('min', 0),
            'maxPrice': event.get('priceRanges', [{}])[0].get('max', 0),
            }
def get_classification(event):
    event_classification = event['classifications'][0]
    return {'id': event['id'],
            'segment': event_classification['segment']['name'],
            'genre': event_classification['genre'].get('name','unknown'),
            'subGenre':event_classification['subGenre'].get('name','unknown')}
def get_venue(event):
    event_venue = event['_embedded']['venues'][0]
    return {'id': event['id'],
            'place':event_venue.get('name',[{}]),
            'address': event_venue['address']['line1'],
            'postalCode': event_venue['postalCode'],
            'city': event_venue['city']['name']}
def get_attractions(event, attract_id):
    event_attraction = event['_embedded']['attractions'][attract_id]
    attraction_classification = event_attraction['classifications'][0]
    return {'id': event['id'],
            'attraction': event_attraction['name'],
            'attraction_segment': attraction_classification['segment']['name'],
            'attraction_genre': attraction_classification['genre'].get('name','unknown'),
            'attraction_subGenre': attraction_classification['subGenre'].get('name','unknown')}
# def json_serializer(obj):
#     if isinstance(obj,uuid.UUID):
#         return str(obj)
#     raise TypeError(f'Object of type {obj.__class__.__name__} is not Json serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivery to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(pro,topic,data):
    pro.produce(topic, key = data['id'], value = json.dumps(data).encode('utf-8'),
                     on_delivery=delivery_report)

def fetch_events():
    try:
        url = f'https://app.ticketmaster.com/discovery/v2/events.json?countryCode=DE&apikey={API_KEY}'
        res = requests.get(url)
        events = res.json()['_embedded']['events']
        return events

    except Exception as e :
        print(f'Fetch error: {e}')


def stream_random_event():
    try:
        events = fetch_events()
        event = random.choice(events)
        event_data = get_event_info(event)
        sales_data = get_sales_data(event)
        classification_data = get_classification(event)
        venue_data = get_venue(event)
        produce_data_to_kafka(producer,EVENT_TOPIC,event_data)
        produce_data_to_kafka(producer,SALES_TOPIC,sales_data)
        produce_data_to_kafka(producer,CLASSIFICATION_TOPIC,classification_data)
        produce_data_to_kafka(producer,VENUE_TOPIC,venue_data)
        i = len(event['_embedded']['attractions'])
        for i in range(0,i):
            attraction_data = get_attractions(event, i)
            produce_data_to_kafka(producer,ATTRACTION_TOPIC,attraction_data)
        producer.flush()

    except Exception as e:
        print(f'Unexpected error: {e}')



if __name__ == "__main__":
    producer_config = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'error_cb': lambda err: print(f'kafka_error: {err}')}
    producer = SerializingProducer(producer_config)
    while True:
        stream_random_event()
        time.sleep(5)

