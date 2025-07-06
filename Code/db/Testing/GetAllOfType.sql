create or replace function pocket.get_All_of_Type_ID(type_id int)
returns as setof pocket.pokemon
language plpgsql
as
$$
declare
    result pocket.pokemon%rowtype;
begin
    for result in select * from pocket.pokemon
    where type_id_1 = type_id or type_id_2 = type_id
    loop
        return next result;
    end loop
    return;
end;
$$;