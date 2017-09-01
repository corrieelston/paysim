import time
from google.cloud import pubsub_v1
from google.cloud import bigquery

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('invesco-fraud-investigation', 'paysim-subscription-1')

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset('paysim')
table = dataset.table('transactions_streaming')
table.reload()

def callback(message):
  print('Received message: {}'.format(message))

  tokens = message.data.decode('utf-8').split(',')
  print(tokens)

  row = (
    int(tokens[0]),
    tokens[1],
    float(tokens[2]),
    tokens[3],
    float(tokens[4]),
    float(tokens[5]),
    tokens[6],
    float(tokens[7]),
    float(tokens[8]),
    int(tokens[9]),
    int(tokens[10])
  )
  print(row)

  table.insert_data([row])

  message.ack()

subscriber.subscribe(subscription_path, callback=callback)
print('Listening for messages on {}'.format(subscription_path))

while True:
  time.sleep(60)
