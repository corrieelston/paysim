from google.cloud import pubsub_v1
from google.cloud import bigquery

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('invesco-fraud-investigation', 'paysim-topic-1')
topic = publisher.create_topic(topic_path)
print('Topic created: {}'.format(topic))

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('invesco-fraud-investigation', 'paysim-subscription-1')
subscription = subscriber.create_subscription(subscription_path, topic_path)
print('Subscription created: {}'.format(subscription))

bigquery_client = bigquery.Client()
dataset = bigquery_client.dataset('paysim')
table = dataset.table('transactions')
table.reload()
table = dataset.table('transactions_streaming', table.schema)
table.create()

