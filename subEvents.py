from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1


project_id = "myproject-65414"
subscription_id_search = "search"
subscription_id_buy = "buy"
subscription_id_book = "book"
# Number of seconds the subscriber should listen for messages
timeout = 5.0

subscriber = pubsub_v1.SubscriberClient()
# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path_book = subscriber.subscription_path(project_id, subscription_id_book)
subscription_path_buy = subscriber.subscription_path(project_id, subscription_id_buy)
subscription_path_search = subscriber.subscription_path(project_id, subscription_id_search)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path_search, callback=callback)
print(f"Listening for messages on {subscription_path_search}..\n")

streaming_pull_future = subscriber.subscribe(subscription_path_buy, callback=callback)
print(f"Listening for messages on {subscription_path_buy}..\n")

streaming_pull_future = subscriber.subscribe(subscription_path_book, callback=callback)
print(f"Listening for messages on {subscription_path_book}..\n")


# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.


# El código de más abajo me parece mejor pero retorna un error al final de la ejecución. 

# from concurrent.futures import TimeoutError
# from google.cloud import pubsub_v1

# project_id = "myproject-65414"
# timeout = 5.0
# subscriber = pubsub_v1.SubscriberClient()


# def callback(message: pubsub_v1.subscriber.message.Message) -> None:
#     print(f"Received {message}.")
#     message.ack()


# subscriptions_ids = ["search", "buy", "book"]
# for subscriptions_id in subscriptions_ids:
#     subscription_path = subscriber.subscription_path(project_id, subscriptions_id)
#     streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
#     print(f"Listening for messages on {subscription_path}..\n")

#     with subscriber:
#         try:
#             streaming_pull_future.result(timeout=timeout)
#         except TimeoutError:
#             streaming_pull_future.cancel() 
#             streaming_pull_future.result()
