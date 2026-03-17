\timing on

-- ============================================
-- LAB 04: Проверка "двойной оплаты"
-- ============================================
--
-- TODO:
-- Замените {{order_id}} на реальный UUID заказа.

-- Сколько раз заказ был переведён в paid
SELECT
    order_id,
    count(*) AS paid_events
FROM order_status_history
WHERE order_id = 'f02c7dfb-e7d3-4263-a4f7-dfd3d5f238ab'::uuid
  AND status = 'paid'
GROUP BY order_id;

-- Детальная история статусов заказа
SELECT
    id,
    order_id,
    status,
    changed_at
FROM order_status_history
WHERE order_id = 'f02c7dfb-e7d3-4263-a4f7-dfd3d5f238ab'::uuid
ORDER BY changed_at;
