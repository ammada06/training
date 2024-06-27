import sqlalchemy as db

def connect_db():
    engine = db.create_engine("postgresql://consultants:WelcomeItc%402022@ec2-3-9-191-104.eu-west-2.compute.amazonaws.com/testdb")
    conn = engine.connect()
    return engine

if __name__ == '__main__':
    engine = connect_db()

    insp = db.inspect(engine)
    print(insp.get_table_names())

    #metadata = db.MetaData()
    #branch = db.Table('branch', metadata, autoload=True, autoload_with=engine)
    #print(branch.columns.keys())

    #stmnt = db.select(branch).where(branch.)