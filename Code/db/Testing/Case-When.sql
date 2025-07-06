\c pkm db_admin

select pokemon_name,
	case when color = 'Yellow' then 'zappy'
		 when color = 'Pink' then 'love you <3'
		 else 'Hi There!'
		 end as message
from pocket.pokemon