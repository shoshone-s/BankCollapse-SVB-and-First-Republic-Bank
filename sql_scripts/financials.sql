CREATE TABLE IF NOT EXISTS financials (
    ID int NOT NULL primary key,
    FOREIGN KEY (company_id) REFERENCES company(FDICCertID),
    report_date            DATE,
    total_assets           BIGINT(255),
    total_liabilities      BIGINT(255),
    total_debt             BIGINT(255),
    assets_return          BIGINT(255),
    equity_return          BIGINT(255),
    efficiency             BIGIGNT(255),
    risk_base_capital_ratio BIGINT(255), 
    symbol                 VARCHAR(255)
);
