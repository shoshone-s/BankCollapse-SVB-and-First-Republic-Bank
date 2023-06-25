from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# from flask import Flask
from dotenv import dotenv_values

# app = Flask(__name__)
# this base is th base class in which the otheres will inheret from
Base = declarative_base()
# this takes all the classes (ie. tables), that extend from base and creates them in the database
# these will connect to the engine and creates all the tables
db_engine = create_engine('sqlite:///bank_collapse.db', echo=True)
Base.metadata.create_all(bind=db_engine)
# this creates a session where we can work with the database
Session = sessionmaker(bind=db_engine)
session = Session()

# Example of working with the database:
#       company = Company(123345, ExampleCompany, 12345678900987654321, 12622020, class-A, active)
#       session.add(company)
#       session.commit()
