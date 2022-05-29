import time
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError


project_id = "myproject-65414"
publisher = pubsub_v1.PublisherClient()
count = 0

f = open("events.csv","r")
while True:
    line = f.readline()
    if not line:
        break


    sleepingTime, topic, message = line.split(",")

    sleepingTime = int(sleepingTime)
    message = message.replace("\n","")

    print("Publishing a topic: '%s' with message: %s"%(topic,message))

    count += 1
    data_str = f"Message number {count}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")

    
    topic_path = publisher.topic_path(project_id, topic)
    future = publisher.publish(topic_path, data)
    print(future.result())

    # print(f"Published messages to {topic_path}.")

    print("Waiting %i seconds"%sleepingTime)
    time.sleep(sleepingTime)

print("Done")
