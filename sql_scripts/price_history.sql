CREATE TABLE IF NOT EXISTS price_history (
    ID int NOT NULL serial primary key,
    symbol          VARCHAR(6) NOT NULL,
    date            DATE,
    "open"          NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    adjusted_close  NUMERIC,
    volume          INTEGER
);