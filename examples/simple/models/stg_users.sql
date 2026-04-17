-- Raw user list. Normally this would read from a CSV seed or a source.
SELECT * FROM (VALUES
    (1, 'Alice',   'alice@example.com',   DATE '2024-01-05'),
    (2, 'Bob',     'bob@example.com',     DATE '2024-02-14'),
    (3, 'Charlie', 'charlie@example.com', DATE '2024-02-21'),
    (4, 'Dana',    'dana@example.com',    DATE '2024-03-02')
) AS t(id, name, email, signed_up_at)
