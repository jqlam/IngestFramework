create role db_admin;

create role dbadmin with login encrypted password 'tbl102';
grant db_admin to dbadmin;