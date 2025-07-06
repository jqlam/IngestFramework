\c pkm dbadmin;

CREATE TABLE pocket.pokemon_raw (
    id              SERIAL,
    pokemon_name    varchar(50) NOT NULL,
    color           varchar(50) NOT NULL,
    type_id_1       varchar(20) NOT NULL,
    type_id_2       varchar(20),
    region          varchar(20) NOT NULL,
    egg_id_1        varchar(20) NOT NULL,
    egg_id_2        varchar(20),
    pre_evo         varchar(50),
    create_ts       timestamp NOT NULL DEFAULT clock_timestamp(),
    update_ts       timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.pokemon_raw owner to db_admin;

alter table pocket.pokemon_raw
    add constraint pk_pokemon_raw        PRIMARY KEY(id);

alter table pocket.pokemon_raw
    add constraint uk_pokemon_raw_name   unique (pokemon_name);
