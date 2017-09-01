import time
from google.cloud import pubsub_v1

FILENAMES = [
  'paysim_26',
  'paysim_27',
  'paysim_28',
  'paysim_29',
  'paysim_30',
  'paysim_31'
]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('invesco-fraud-investigation', 'paysim-topic-1')

for filename in FILENAMES:
  print filename
        
  with open(filename, 'r') as file:
    file.readline()
                      
    for line in file:
      print line
                                          
      data = line.encode('utf-8')
      publisher.publish(topic_path, data=data)

      time.sleep(0.5)
