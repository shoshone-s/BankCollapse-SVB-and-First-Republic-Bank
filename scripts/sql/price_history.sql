CREATE TABLE IF NOT EXISTS price_history (
    ID              INTEGER IDENTITY(0,1),
    symbol          VARCHAR(6) NOT NULL,
    date            DATE,
    "open"          DECIMAL(6,2),
    high            DECIMAL(6,2),
    low             DECIMAL(6,2),
    close           DECIMAL(6,2),
    adjusted_close  DECIMAL(6,2),
    volume          INTEGER,
    primary key(ID)
);