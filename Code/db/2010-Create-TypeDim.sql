\c pkm dbadmin;

CREATE TABLE pocket.type_dim (
    id          int NOT NULL,
    type_name   varchar(20) NOT NULL,
    create_ts   timestamp NOT NULL DEFAULT clock_timestamp(),
    update_ts   timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.type_dim owner to db_admin;

alter table pocket.type_dim
    add constraint type_dim_pk primary key(id);

-- create trigger tub_type_dim_update_ts
--     before update on pocket.type_dim
--     for each row
--         execute procedure pocket.set_update_ts();