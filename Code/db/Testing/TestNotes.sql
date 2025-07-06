\c pkm dbadmin

select
	p.id, p.pokemon_name, p.color, p.type_id_1, td.type_name, p.type_id_2, td2.type_name,
	p.region, rd.region_name, p.egg_id_1, ed.egg_name, p.egg_id_2, ed2.egg_name, p.pre_evo, p.create_ts, p.update_ts
from pocket.pokemon p
inner join pocket.type_dim td
	on p.type_id_1 = td.id
left outer join pocket.type_dim td2
	on p.type_id_2 = td2.id
inner join pocket.egg_dim ed 
	on p.egg_id_1 = ed.id
left outer join pocket.egg_dim ed2 
	on p.egg_id_2  = ed2.id
left outer join pocket.region_dim rd 
	on p.region = rd.id