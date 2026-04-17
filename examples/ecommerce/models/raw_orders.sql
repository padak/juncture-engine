SELECT * FROM (VALUES
    (1001, 1, DATE '2024-01-10', 'completed',  89.90),
    (1002, 1, DATE '2024-02-12', 'completed', 199.00),
    (1003, 2, DATE '2024-01-25', 'refunded',   45.00),
    (1004, 2, DATE '2024-03-30', 'completed', 129.50),
    (1005, 3, DATE '2024-02-14', 'completed',  29.99),
    (1006, 4, DATE '2024-02-20', 'completed', 749.00),
    (1007, 4, DATE '2024-04-01', 'completed', 299.00),
    (1008, 5, DATE '2024-03-02', 'completed',  59.00),
    (1009, 5, DATE '2024-03-28', 'completed',  59.00),
    (1010, 6, DATE '2024-03-22', 'pending',   499.00),
    (1011, 7, DATE '2024-04-05', 'completed', 129.00)
) AS t(order_id, customer_id, order_date, status, amount)
