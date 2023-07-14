CREATE TABLE IF NOT EXISTS price_history (
    ID int NOT NULL serial primary key,
    symbol          VARCHAR(6) NOT NULL,
    date            DATE,
    "open"          FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    adjusted_close  FLOAT,
    volume          INTEGER
);