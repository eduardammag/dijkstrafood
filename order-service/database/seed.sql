-- USERS
INSERT INTO users (user_id, user_name, user_type)
VALUES
    (1, 'João', 'client'),
    (2, 'Maria', 'admin'),
    (3, 'Pedro', 'courier')
ON CONFLICT (user_id) DO NOTHING;

-- COURIER
INSERT INTO couriers (user_id, vehicle_type, is_available)
VALUES (3, 'bike', TRUE)
ON CONFLICT (user_id) DO NOTHING;

-- RESTAURANT
INSERT INTO restaurants (restaurant_id, restaurant_name, creator_user_id)
VALUES (1, 'Pizza Top', 2)
ON CONFLICT (restaurant_id) DO NOTHING;

-- ORDER
INSERT INTO orders (order_id, client_id, restaurant_id, courier_id, order_status)
VALUES (1, 1, 1, 3, 'CONFIRMED')
ON CONFLICT (order_id) DO NOTHING;

-- ORDER EVENT
INSERT INTO order_events (order_id, event_type, from_status, to_status, event_message)
SELECT 1, 'STATUS_CHANGE', NULL, 'CONFIRMED', 'Pedido inicial confirmado'
WHERE NOT EXISTS (
    SELECT 1
    FROM order_events
    WHERE order_id = 1 AND event_type = 'STATUS_CHANGE' AND to_status = 'CONFIRMED'
);

-- ORDER ITEMS
INSERT INTO order_items (order_id, item_name, quantity)
SELECT 1, 'Pizza Calabresa', 1
WHERE NOT EXISTS (
    SELECT 1
    FROM order_items
    WHERE order_id = 1 AND item_name = 'Pizza Calabresa' AND quantity = 1
);
