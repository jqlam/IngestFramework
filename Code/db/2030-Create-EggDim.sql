\c pkm dbadmin;

CREATE TABLE pocket.egg_dim (
    id int NOT NULL,
    egg_name varchar(20) NOT NULL,
    create_ts timestamp NOT NULL DEFAULT clock_timestamp(),
    update_ts timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.egg_dim owner to db_admin;

alter table pocket.egg_dim
    add constraint egg_dim_pk primary key(id);

-- create trigger tub_egg_dim_update_ts
--     before update on pocket.egg_dim
--     for each row
--         execute procedure pocket.set_update_ts();