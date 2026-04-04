from broker import Broker

broker = Broker()

message = {
    "event": "delivery.assigned",
    "order_id": "order-001",
    "driver_id": 7,
    "restaurant_id": 1,
    "client_id": 1,
    "pickup_location": {
        "lat": -22.1200,
        "lng": -51.3900
    },
    "delivery_location": {
        "lat": -22.1100,
        "lng": -51.4100
    }
}

routing_key = broker.get_courier_routing_key(7)
queue_name = broker.get_courier_queue_name(7)

broker.declare_queue_and_bind(queue_name, [routing_key])
broker.publish(routing_key, message)
broker.close()