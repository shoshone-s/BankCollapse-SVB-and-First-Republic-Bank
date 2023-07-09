CREATE TABLE IF NOT EXISTS sec_data (
    asset_num          VARCHAR(255),
    FOREIGN KEY (company_id) REFERENCES company(company_id),
    report_type        VARCHAR(255),
    start_date         DATE,
    end_date           DATE,
    date_filed         DATE,
    fiscal_year        TINYINT(4),
    fiscal_period      TINYINT(2),
    form               VARCHAR(255),
    frame              VARCHAR(255),
    value              VARCHAR(255)
);