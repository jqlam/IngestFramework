\c pkm dbadmin;

CREATE TABLE pocket.region_dim (
    id int NOT NULL,
    region_name varchar(20) NOT NULL,
    create_ts timestamp NOT NULL DEFAULT clock_timestamp(),
    update_ts timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.region_dim owner to db_admin;

alter table pocket.region_dim
    add constraint region_dim_pk primary key(id);

-- create trigger tub_region_dim_update_ts
--     before update on pocket.region_dim
--     for each row
--         execute procedure pocket.set_update_ts();