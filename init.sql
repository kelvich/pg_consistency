DROP TABLE IF EXISTS accounts;

CREATE TABLE accounts
(
    id serial NOT NULL,
    balance integer,
    CONSTRAINT userinfo_pkey PRIMARY KEY (id)
) DISTRIBUTE BY hash(id);

insert into accounts(id, balance) (select s, 100000*random() from generate_series(1,10000) as s);

SELECT sum(balance) FROM accounts;


