import json
import pika

credentials = pika.PlainCredentials("guest", "guest")
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost", port=5672, credentials=credentials)
)
channel = connection.channel()

exchange = "dijkfood"
channel.exchange_declare(exchange=exchange, exchange_type="topic", durable=True)

restaurant_message = {
    "event": "restaurant.order.created",
    "order_id": "ord_1",
    "restaurant_id": "rest_1",
    "pickup_location": {"lat": -23.56, "lng": -46.65}
}

courier_message = {
    "event": "courier.assigned",
    "order_id": "ord_1",
    "courier_id": "courier_1",
    "restaurant_id": "rest_1",
    "pickup_location": {"lat": -23.56, "lng": -46.65},
    "dropoff_location": {"lat": -23.55, "lng": -46.63},
    "path": [
        {"lat": -23.5600, "lng": -46.6500},
        {"lat": -23.5580, "lng": -46.6460},
        {"lat": -23.5560, "lng": -46.6420},
        {"lat": -23.5540, "lng": -46.6380},
        {"lat": -23.5500, "lng": -46.6300},
    ]
}

channel.basic_publish(
    exchange=exchange,
    routing_key="restaurant.order.created",
    body=json.dumps(restaurant_message).encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2)
)

channel.basic_publish(
    exchange=exchange,
    routing_key="courier.assigned",
    body=json.dumps(courier_message).encode("utf-8"),
    properties=pika.BasicProperties(delivery_mode=2)
)

connection.close()
print("Mensagens enviadas.")