from broker import Broker

broker = Broker()

order = {
    "order_id": "order-001",
    "client_id": 1,
    "restaurant_id": 1,
    "items": [
        {"name": "Pizza", "quantity": 2},
        {"name": "Refrigerante", "quantity": 1}
    ],
    "pickup_location": {
        "lat": -22.1200,
        "lng": -51.3900
    },
    "delivery_location": {
        "lat": -22.1100,
        "lng": -51.4100
    }
}

broker.publish_order(order)
broker.close()