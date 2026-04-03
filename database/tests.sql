-- Ver usuários
SELECT * FROM users;

-- Ver pedidos com relacionamento
SELECT
    o.order_id,
    u.user_name AS client,
    r.restaurant_name,
    o.order_status,
    o.created_at
FROM orders o
JOIN users u ON o.client_id = u.user_id
JOIN restaurants r ON o.restaurant_id = r.restaurant_id;

-- Ver eventos do pedido
SELECT * FROM order_events;

-- Ver itens do pedido
SELECT * FROM order_items;