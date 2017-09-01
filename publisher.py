import os
import time
from google.cloud import pubsub_v1

PROJECT = os.environ['PROJECT']

FILENAMES = [
  '../streaming/paysim_26',
  '../streaming/paysim_27',
  '../streaming/paysim_28',
  '../streaming/paysim_29',
  '../streaming/paysim_30',
  '../streaming/paysim_31'
]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, 'paysim-topic-1')

for filename in FILENAMES:
  print filename
        
  with open(filename, 'r') as file:
    file.readline()
                      
    for line in file:
      print line
                                          
      data = line.encode('utf-8')
      publisher.publish(topic_path, data=data)

      time.sleep(0.5)
