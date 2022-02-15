BEGIN;

  CREATE TABLE IF NOT EXISTS instruments (
    instrument_id NUMERIC PRIMARY KEY,
    sector_name TEXT NOT NULL,
    country_name TEXT NOT NULL,
    index_name TEXT NOT NULL,
    instrument_type TEXT NOT NULL 
  );

  CREATE TABLE IF NOT EXISTS prices (
    date VARCHAR(8) NOT NULL,
    price NUMERIC NOT NULL,
    instrument_id NUMERIC NOT NULL REFERENCES instruments(instrument_id),
    UNIQUE (date, instrument_id)
  );

  CREATE TABLE IF NOT EXISTS trades (
    customer_id NUMERIC NOT NULL,
    execution_time TEXT NULL,
    direction TEXT NOT NULL,
    execution_size NUMERIC NOT NULL,
    execution_price NUMERIC NOT NULL,
    instrument_id NUMERIC NOT NULL REFERENCES instruments(instrument_id),
    UNIQUE (customer_id, execution_time)
  );

COMMIT;
