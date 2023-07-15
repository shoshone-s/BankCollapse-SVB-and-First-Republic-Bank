CREATE TABLE IF NOT EXISTS sec_data (
    ID                INTEGER,
    symbol            VARCHAR(6),
    asset_num         VARCHAR(255),
    start_date        DATE,
    end_date          DATE,
    date_filed        DATE,
    fiscal_year       INTEGER,
    fiscal_period     VARCHAR(2),
    form              VARCHAR(255),
    frame             VARCHAR(255),
    value             DECIMAL(19,0),
    primary key(ID)
);