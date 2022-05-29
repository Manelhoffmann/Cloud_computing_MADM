import time
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError


project_id = "myproject-65414"
topic_id_search = "search"
topic_id_buy = "buy"
topic_id_book = "book"

publisher = pubsub_v1.PublisherClient()
topic_path_search = publisher.topic_path(project_id, topic_id_search)
topic_path_buy = publisher.topic_path(project_id, topic_id_buy)
topic_path_book = publisher.topic_path(project_id, topic_id_book)
count = 0

f = open("events.csv","r")
while True:
    line = f.readline()
    if not line:
        break


    sleepingTime,topic,message = line.split(",")

    sleepingTime = int(sleepingTime)
    message = message.replace("\n","")

    print("Publishing a topic: '%s' with message: %s"%(topic,message))

    count += 1
    data_str = f"Message number {count}"
    # Data must be a bytestring
    data = data_str.encode("utf-8")
    
    # TODO PUB 
    if topic == "search":
        future_search = publisher.publish(topic_path_search, data)
        print(future_search.result())
    if topic == "buy":
        future_buy = publisher.publish(topic_path_buy, data)
        print(future_buy.result())
    if topic == "book":
        future_book = publisher.publish(topic_path_book, data)
        print(future_book.result())

    # print(f"Published messages to {topic_path}.")

    print("Waiting %i seconds"%sleepingTime)
    time.sleep(sleepingTime)

print("Done")

