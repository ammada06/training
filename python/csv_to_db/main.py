import pandas
import sqlalchemy as db

def read_csv():
    return pandas.read_csv(r"C:\Users\Ammad Ashraf\Documents\code\python-src\csv_to_db\data.csv")

def connect_db():
    engine = db.create_engine("mysql://ammada:public@localhost:3306/test")
    conn = engine.connect()
    return engine

if __name__ == '__main__':
    engine = connect_db()
    df = read_csv()
    df.to_sql("airbnb_listings", engine, if_exists='replace')

# if __name__ == '__main__':
#     df = read_csv()
#     metadata = db.MetaData()
#     Listing = db.Table('airbnb_listings', metadata, 
#                     db.Column('id', db.Integer()),
#                     db.Column('city', db.String(255)),
#                     db.Column("country", db.String(255)),
#                     db.Column("number_of_rooms", db.Integer),
#                     db.Column("date_listed", db.Integer)
#     )
#     conn = connect_db()
#     for ind in df.index:
#         print(df["Id"][ind], df["City"][ind], df["Country"][ind])
#         query = db.insert(Listing).values(id=df["Id"][ind],
#                                             city=df["City"][ind],
#                                             country=df["Country"][ind],
#                                             number_of_rooms=df["Rooms"][ind],
#                                             date_listed=df["Year"][ind]
#                                         )
#         Result = conn.execute(query)
#     output = conn.execute(Listing.select()).fetchall()
#     conn.commit()
#     print(output)

