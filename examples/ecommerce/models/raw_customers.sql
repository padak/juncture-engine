SELECT * FROM (VALUES
    (1, 'Anna',   'anna@acme.com',   'CZ', DATE '2023-07-15'),
    (2, 'Bob',    'bob@acme.com',    'US', DATE '2023-09-02'),
    (3, 'Cora',   'cora@acme.com',   'CZ', DATE '2024-01-11'),
    (4, 'Denis',  'denis@acme.com',  'DE', DATE '2024-02-18'),
    (5, 'Eva',    'eva@acme.com',    'US', DATE '2024-03-01'),
    (6, 'Franta', 'franta@acme.com', 'CZ', DATE '2024-03-20'),
    (7, 'Gina',   'gina@acme.com',   'DE', DATE '2024-04-04')
) AS t(customer_id, name, email, country, signed_up_at)
