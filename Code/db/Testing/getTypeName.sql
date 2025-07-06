create or replace function pocket.getTypeName(type_id int)
returns varchar(20)
language plpgsql
as
$$
declare
    result varchar(20);
begin
    select type_name
    into result
    from pocket.type_dim
    where id = type_id;

    return result;
end;
$$;