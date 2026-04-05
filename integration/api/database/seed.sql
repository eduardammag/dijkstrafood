-- USERS
INSERT INTO users (user_name, user_type) VALUES ('João', 'client');
INSERT INTO users (user_name, user_type) VALUES ('Maria', 'admin');
INSERT INTO users (user_name, user_type) VALUES ('Pedro', 'courier');

-- COURIER
INSERT INTO couriers (user_id, vehicle_type) VALUES (3, 'bike');

-- RESTAURANT
INSERT INTO restaurants (restaurant_name, creator_user_id)
VALUES ('Pizza Top', 2);

-- ORDER
INSERT INTO orders (client_id, restaurant_id, courier_id, order_status)
VALUES (1, 1, 3, 'CONFIRMED');
INSERT INTO orders (client_id, restaurant_id, courier_id, order_status)
VALUES (1, 1, 3, 'CONFIRMED');

-- ORDER EVENT
INSERT INTO order_events (order_id, event_status)
VALUES (1, 'CONFIRMED');

-- ORDER ITEMS
INSERT INTO order_items (order_id, item_name, quantity)
VALUES (1, 'Pizza Calabresa', 1);