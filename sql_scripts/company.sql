CREATE TABLE Company (
	ID int NOT NULL primary key, --setting primary key in postgres using serial keyword
	Name varchar(255),
	FDICCertID int NOT NULL,
	Category varchar(255),
	Status varchar(255),
	Symbol varchar(255),
	EstablishedDate date,
	Sector varchar(255),
	Industry varchar(255)

);