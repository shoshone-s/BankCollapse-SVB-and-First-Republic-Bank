CREATE TABLE IF NOT EXISTS debt_to_equity (
    ID                              INTEGER IDENTITY(0,1),
    symbol                          VARCHAR(6) NOT NULL,
    date                            DATE,
    debt_to_equity_ratio            FLOAT,
    long_term_debt_curr_symbol      VARCHAR(1),
    long_term_debt                  FLOAT,
    long_term_debt_uom              VARCHAR(1),
    shareholder_equity_curr_symbol  VARCHAR(1),
    shareholder_equity              FLOAT,
    shareholder_equity_uom          VARCHAR(1),
    primary key(ID)
);