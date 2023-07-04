CREATE TABLE Company (
	ID int NOT NULL serial primary key, --setting primary key in postgres using serial keyword
	Name varchar(60),
	SymbolID int NOT NULL,
	FDICCertID int NOT NULL,
	Class varchar(60),
	Status varchar(60),
	FOREIGN KEY (SymbolID) REFERENCES Symbol(ID)
);