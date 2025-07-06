\c pkm dbadmin;

CREATE TABLE pocket.control_table (
    stage_min_batch     int not null,
    stage_max_batch     int not null,
    ingest_min_batch    int not null,
    ingest_max_batch    int not null
);

alter table if exists pocket.control_table owner to db_admin;