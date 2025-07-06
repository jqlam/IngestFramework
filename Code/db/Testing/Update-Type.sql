create or replace procedure pocket.update_color(p_name varchar, new_color varchar)
language plpgsql
as
$$
begin
    update pocket.pokemon
        set color = new_color
        where pokemon_name = p_name;
    return;
end;
$$;