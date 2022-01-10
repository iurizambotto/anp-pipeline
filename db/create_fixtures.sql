CREATE TABLE IF NOT EXISTS anp_updates (
    id SERIAL,
    keyword VARCHAR,
    last_update TIMESTAMP,
    created_at TIMESTAMP DEFAULT now()
)
;

CREATE TABLE IF NOT EXISTS vendas_derivados_petroleo (
    id SERIAL,
    year_month DATE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    product VARCHAR NOT NULL,
    unit VARCHAR NOT NULL,
    volume DECIMAL NOT NULL,
    created_at TIMESTAMP DEFAULT now()
)
;

CREATE TABLE IF NOT EXISTS vendas_oleo_diesel (
    id SERIAL,
    year_month DATE NOT NULL,
    uf VARCHAR(2) NOT NULL,
    product VARCHAR NOT NULL,
    unit VARCHAR NOT NULL,
    volume DECIMAL NOT NULL,
    created_at TIMESTAMP DEFAULT now()
)
;