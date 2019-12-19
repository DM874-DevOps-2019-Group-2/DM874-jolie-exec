-- DROP DATABASE IF EXISTS jolie_exec_db;

CREATE DATABASE jolie_exec_db;

\c jolie_exec_db;

CREATE TABLE jolie_exec (
    user_id integer PRIMARY KEY,
    has_recv_script boolean NOT NULL DEFAULT(false),
    has_send_script boolean NOT NULL DEFAULT(false)
);