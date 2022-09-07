from kafka import KafkaProducer
import json
import requests
from time import sleep

KAFKA_TOPIC = 'yad2'


def _produce():
    brokers = ['Cnt7-naya-cdh63:9092']
    producer = KafkaProducer(bootstrap_servers=brokers)

    url = 'https://gw.yad2.co.il/feed-search-legacy/realestate/forsale?priceOnly=1&page=4&forceLdLoad=true'
    # produce
    while True:
        response = requests.get(url)
        data = response.json()

        for line in data['data']['feed']['feed_items']:
            try:
                dict1 = {'record_id': line['record_id'], 'ad_number': line['ad_number'],
                         'rooms': line['row_4'][0]['value'],
                         'floor': line['row_4'][1]['value'], 'SquareMeter': line['row_4'][2]['value'],
                         'price': int(line['price'][:-2].replace(',', '').strip()), 'currency': line['currency'],
                         'city_code': line['city_code'], 'city': line['city'], 'street': line['street'],
                         'AssetClassificationID_text': line['AssetClassificationID_text'],
                         'coordinates': line['coordinates'],
                         'date': line['date'], 'date_added': line['date_added']}
                if dict1['floor'] == 'קרקע':
                    dict1['floor'] = 0
                kafka_message = dict1
                producer.send(topic=KAFKA_TOPIC, value=json.dumps(kafka_message).encode('ISO-8859-8'))
                producer.flush()
                print(kafka_message)
                sleep(4)
            except:
                pass
        print('sleeping for 10')
        sleep(10)


def main():
    _produce()


if __name__ == '__main__':
    main()
