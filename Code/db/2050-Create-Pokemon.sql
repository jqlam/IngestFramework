\c pkm dbadmin;

CREATE TABLE pocket.pokemon (
    id              SERIAL,
    pokemon_name    varchar(50) NOT NULL,
    color           varchar(50) NOT NULL,
    type_id_1       int NOT NULL,
    type_id_2       int,
    region          int NOT NULL,
    egg_id_1        int NOT NULL,
    egg_id_2        int,
    pre_evo         varchar(50),
    create_ts       timestamp NOT NULL DEFAULT clock_timestamp(),
    update_ts       timestamp NOT NULL DEFAULT clock_timestamp()
);

alter table if exists pocket.pokemon owner to db_admin;

alter table pocket.pokemon
    add constraint pk_pokemon        PRIMARY KEY(id);

alter table pocket.pokemon
    add constraint fk_pokemon_type_1 foreign key (type_id_1) REFERENCES pocket.type_dim   (id);

alter table pocket.pokemon
    add constraint fk_pokemon_type_2 foreign key (type_id_2) REFERENCES pocket.type_dim   (id);

alter table pocket.pokemon
    add constraint fk_region         foreign key (region)    REFERENCES pocket.region_dim (id);

alter table pocket.pokemon
    add constraint fk_egg_1          foreign key (egg_id_1)  REFERENCES pocket.egg_dim    (id);

alter table pocket.pokemon
    add constraint fk_egg_2          foreign key (egg_id_2)  REFERENCES pocket.egg_dim    (id);