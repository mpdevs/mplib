create sequence text_value_id increment by 1 minvalue 1 no maxvalue start with 1;

create table text_value
(
	id int8 default nextval('text_value_id') primary key ,
	name varchar(200),
	value text
);
