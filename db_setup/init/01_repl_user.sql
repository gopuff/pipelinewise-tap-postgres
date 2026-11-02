-- Non-superuser the tests run the tap as, mirroring a production replication user.
CREATE ROLE repl_user WITH LOGIN REPLICATION PASSWORD 'repl_password';
-- Postgres 15+ no longer lets every role create objects in public.
GRANT ALL ON SCHEMA public TO repl_user;
CREATE DATABASE tap_postgres_test OWNER repl_user;
