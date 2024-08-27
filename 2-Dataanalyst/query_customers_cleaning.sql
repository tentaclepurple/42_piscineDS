CREATE TABLE customers2 AS
WITH event_sequence AS (
    SELECT *,
           SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) 
               OVER (PARTITION BY user_session, product_id ORDER BY event_time) as cart_count,
           SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) 
               OVER (PARTITION BY user_session, product_id ORDER BY event_time) as remove_count
    FROM public.customers
    WHERE event_type IN ('cart', 'remove_from_cart', 'purchase', 'view')
),
valid_events AS (
    SELECT *,
           cart_count - remove_count as cart_status,
           CASE 
               WHEN event_type = 'remove_from_cart' AND cart_count > remove_count - 1 THEN 1
               ELSE 0
           END as valid_remove
    FROM event_sequence
)
SELECT *
FROM valid_events
WHERE event_type = 'view'
   OR event_type = 'cart'
   OR (event_type = 'remove_from_cart' AND valid_remove = 1)
   OR (event_type = 'purchase' AND cart_status > 0)
ORDER BY user_session, user_id, event_time;