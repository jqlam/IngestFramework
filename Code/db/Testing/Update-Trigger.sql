create trigger trg_update_ts
before update on pocket.pokemon
for each row
execute function update_timestamp()