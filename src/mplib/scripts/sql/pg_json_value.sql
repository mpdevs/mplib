create sequence json_value_id_seq increment by 1 minvalue 1 no maxvalue start with 1;

create table json_value
(
    id int8 default nextval('json_value_id_seq') primary key,
	name varchar(200),
	value json
);
