import sqlite3
import csv


db_name = "casino.db"

create_table_queries = [
"""
CREATE TABLE GAMES(
   id_game        INTEGER PRIMARY KEY     NOT NULL,
   game           TEXT     NOT NULL,
   rtp            REAL     NOT NULL,
   volatility     INTEGER NOT NULL
);
""",
"""
CREATE TABLE ORGANIZATIONS(
   id_organization 		INTEGER PRIMARY KEY     NOT NULL,
   organization         TEXT    NOT NULL,
   organization_group   TEXT    NOT NULL
);
""",
"""
CREATE TABLE FACT_BETS(
   id_bet			INTEGER PRIMARY KEY NOT NULL,
   date			TEXT,
   slice_from		TEXT,
   organization	TEXT,
   id_game			INTEGER,
   channel			TEXT,
   currency		CHAR(3),
   bet_euro		REAL,
   win_euro		REAL,
   number_of_bets INTEGER,
   userid			TEXT,
   campaignid		INTEGER
);
"""]

# order of files like inserts!
files = ["games.csv", "organizations.csv", "fact_bets.csv"]

insert_queries = [
"""
INSERT INTO GAMES(
    id_game,
    game,
    rtp,
    volatility)
VALUES (?,?,?,?);
""",
"""
INSERT INTO ORGANIZATIONS(
    organization,
    organization_group)
VALUES (?,?);
""",
"""
INSERT INTO FACT_BETS(
    date,
    slice_from,
    organization,
    id_game,
    channel,
    currency,
    bet_euro,
    win_euro,
    number_of_bets,
    userid,
    campaignid)
VALUES (?,?,?,?,?,?,?,?,?,?,?);
"""]


def create_table(create_query, db_name):
    with sqlite3.connect(db_name) as conn:
        c = conn.cursor()
        c.execute(create_query)
        conn.commit()


def insert_to_table_from_csv(file_name, insert_query, db_name):
    with open(file_name) as f:
        reader = csv.reader(f, delimiter=';')
        next(reader)
        with sqlite3.connect(db_name) as conn:
            c = conn.cursor()
            for r in reader:
                c.execute(insert_query, r)
            conn.commit()


for q in create_table_queries:
    create_table(q, db_name)

for f, q in zip(files, insert_queries):
    insert_to_table_from_csv(f, q, db_name)
