-- Initialisation script executed once when the postgres container is first created.
-- Creates one database per service and a shared application role.

CREATE USER app WITH PASSWORD 'app';

CREATE DATABASE gateway    WITH OWNER app;
CREATE DATABASE orders     WITH OWNER app;
CREATE DATABASE payments   WITH OWNER app;
CREATE DATABASE notifications WITH OWNER app;

\connect gateway
GRANT ALL PRIVILEGES ON SCHEMA public TO app;

\connect orders
GRANT ALL PRIVILEGES ON SCHEMA public TO app;

\connect payments
GRANT ALL PRIVILEGES ON SCHEMA public TO app;

\connect notifications
GRANT ALL PRIVILEGES ON SCHEMA public TO app;
