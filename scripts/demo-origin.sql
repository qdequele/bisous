-- Demo data for the `origin` service of compose.yaml.
CREATE TABLE products (
    id          bigint PRIMARY KEY,
    name        text NOT NULL,
    description text,
    price_cents integer NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO products (id, name, description, price_cents)
SELECT g, 'Product ' || g, 'A demo product number ' || g, (g * 37) % 10000
FROM generate_series(1, 1000) AS g;

-- The replication role the sync uses (the owner of the table here).
ALTER ROLE app WITH REPLICATION;
