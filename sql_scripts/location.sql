CREATE TABLE IF NOT EXISTS location (
    cert               INTEGER NOT NULL,
    company_name       VARCHAR(255),
    main_office        BOOL,
    branch_name        VARCHAR(255),
    established_date   DATE,
    service_type       VARCHAR(255),
    address            VARCHAR(255),
    county             VARCHAR(255),
    city               VARCHAR(255),
    state              VARCHAR(255),
    zip                VARCHAR(5),
    latitude           NUMERIC,
    longitude          NUMERIC
);