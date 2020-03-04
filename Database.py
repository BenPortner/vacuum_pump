import subprocess
import sqlalchemy as sqla
import sqlalchemy.ext.automap

def start_session(
    database = "bonsai",
    username = "bonsai",
    password = "",
    url = "localhost",
    port = "5432",
):

    # start psql server
    try:
        r = subprocess.check_output("pg_ctl status")
    except subprocess.CalledProcessError as e:
        subprocess.call("pg_ctl -l logfile start")

    # connect to database
    db = sqla.create_engine("postgresql://"+username+":"+password+"@"+url+":"+port+"/"+database+"", echo=True)

    # get database metadata
    meta = sqla.MetaData()
    meta.reflect(db)

    # create one class for each table in the database
    Base = sqlalchemy.ext.automap.automap_base(metadata=meta)
    Base.prepare()

    # create a session
    session = sqla.orm.sessionmaker(bind=db)()

    return session, Base, Base.classes, meta, db