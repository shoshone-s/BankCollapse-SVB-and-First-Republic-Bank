CREATE TABLE IF NOT EXISTS financials (
    FOREIGN KEY (company_id) REFERENCES company(company_id),
    report_date            DATE,
    total_assets           BIGINT(255),
    total_liabilities      BIGINT(255),
    total_debt             BIGINT(255),
    assets_return          BIGINT(255),
    equity_return          BIGINT(255),
    efficiency             BIGIGNT(255),
    risk_base_capital_ratio BIGINT(255)
);
