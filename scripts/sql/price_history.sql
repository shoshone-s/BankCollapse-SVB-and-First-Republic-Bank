CREATE TABLE IF NOT EXISTS price_history (
    ID              INTEGER IDENTITY(0,1),
    symbol          VARCHAR(6) NOT NULL,
    date            DATE,
    "open"          FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    adjusted_close  FLOAT,
    volume          INTEGER,
    primary key(ID)
);