\c pkm dbadmin;

insert into pocket.pokemon (
    pokemon_name,
    color,
    type_id_1,
    type_id_2,
    region,
    egg_id_1,
    egg_id_2,
    pre_evo
)
select 
    pr.pokemon_name, 
    pr.color, 
    --pr.type_id_1, 
    td.id, 
    --pr.type_id_2,
    td2.id, 
    --pr.region, 
	rd.id, 
    --pr.egg_id_1, 
    ed.id, 
    --pr.egg_id_2, 
    ed2.id, 
    pr.pre_evo
from pocket.pokemon_raw pr
inner join pocket.type_dim td
    on pr.type_id_1 = td.type_name
left outer join pocket.type_dim td2
    on pr.type_id_2 = td2.type_name
inner join pocket.region_dim rd
    on pr.region = rd.region_name
inner join pocket.egg_dim ed
    on pr.egg_id_1 = ed.egg_name
left outer join pocket.egg_dim ed2
    on pr.egg_id_2 = ed2.egg_name
order by pr.id
    
    