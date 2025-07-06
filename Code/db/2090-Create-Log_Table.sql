\c pkm dbadmin;

CREATE TABLE pocket.log_table (
    id          serial,
    invoker     text,
    log_level   text,
    log_message text,
    create_date timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.log_table owner to db_admin;
