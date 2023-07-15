CREATE TABLE IF NOT EXISTS company (
    ID                INTEGER IDENTITY(0,1),
    symbol            VARCHAR(6),
    fdic_cert_id      INTEGER,
    name              VARCHAR(255),
    category          VARCHAR(255),
    status            VARCHAR(255),
    established_date  DATE,
    exchange          VARCHAR(255),
    sector            VARCHAR(255),
    primary key(ID)
);