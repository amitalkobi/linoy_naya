# First part
from kafka import KafkaProducer
import json
import requests
from time import sleep
from kafka.admin import KafkaAdminClient, NewTopic

brokers = ['34.71.172.85:9092']
# admin_client = KafkaAdminClient(
#     bootstrap_servers=brokers,
#     client_id='test'
# )


topic1 = 'yad2'
# topic_list = []
# topic_list.append(NewTopic(name=topic1, num_partitions=1, replication_factor=1))
# admin_client.create_topics(new_topics=topic_list, validate_only=False)


producer = KafkaProducer(bootstrap_servers=brokers,
                         api_version=(0,11,5),
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

url = 'https://gw.yad2.co.il/feed-search-legacy/realestate/forsale?priceOnly=1&page=4&forceLdLoad=true'
response = requests.get(url)
data = response.json()
# data

for line in data['data']['feed']['feed_items']:
  print("in for")
  try:
    dict1 = { 'record_id': line['record_id'], 'ad_number': line['ad_number'], 'rooms': line['row_4'][0]['value'], 'floor': line['row_4'][1]['value'], 'SquareMeter': line['row_4'][2]['value'], 'price': int(line['price'][:-2].replace(',','').strip()), 'currency': line['currency'], 'city_code': line['city_code'], 'city': line['city'], 'street': line['street'], 'AssetClassificationID_text': line['AssetClassificationID_text'], 'coordinates': line['coordinates'], 'date': line['date'], 'date_added': line['date_added'] }
    if dict1['floor']== 'קרקע':
      dict1['floor']=0
    kafka_message = dict1
    producer.send(topic=topic1, value=json.dumps(kafka_message).encode('ISO-8859-8'))
    producer.flush()
    print(kafka_message)
    sleep(4)
  except Exception as e:
    pass


