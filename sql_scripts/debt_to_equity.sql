CREATE TABLE DebtToEquity (
	ID int NOT NULL serial primary key,
	Date date not null,
	LongTermDebt numeric,
	DebtEquityRatio numeric,
);
