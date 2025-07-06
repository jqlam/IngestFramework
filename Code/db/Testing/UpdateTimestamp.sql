create function update_timestamp()
returns trigger 
language plpgsql
as 
$$
begin
    new.update_ts = now();
    RETURN new;
end;
$$