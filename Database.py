import subprocess
import sqlalchemy as sqla
import sqlalchemy.ext.automap

class Database():

    current_session = None
    tables = None
    base_class = None
    metadata = None
    engine = None

    def __init__(self, **kwargs):
        self.start_session(**kwargs)

    # connect to the database, information about the tables and return a ORM class for each table
    def start_session(
            self, database = "bonsai", username = "bonsai", password = "",
            url = "localhost", port = "5432"
    ):

        # start psql server
        try:
            r = subprocess.check_output("pg_ctl status")
        except subprocess.CalledProcessError as e:
            subprocess.call("pg_ctl -l logfile start")

        # connect to database
        engine = sqla.create_engine("postgresql://"+username+":"+password+"@"+url+":"+port+"/"+database+"", echo=True)

        # get database metadata
        metadata = sqla.MetaData()
        metadata.reflect(engine)

        # create one class for each table in the database
        base_class = sqlalchemy.ext.automap.automap_base(metadata=metadata)
        base_class.prepare()

        # create a session
        session = sqla.orm.sessionmaker(bind=engine)()

        # save handles
        self.current_session = session
        self.tables = base_class.classes
        self.base_class = base_class
        self.metadata = metadata
        self.engine = engine

        return session, base_class.classes


    # default method for adding entries to tables where the id is an integer
    def add_entry(self, table, entry_data):
        # auto-generate id
        new_id = self.current_session.query(table).count() + 1
        # add id field to argument dict
        entry_data["id"] = new_id
        # create table entry
        return table(**entry_data)


    # helper function to check which exiobase locations are already contained in the database
    # right now rather stupid (check by matching database identifiers with exiobase country codes)
    def match_exiobase_locations(self, df_exiobase_locations):
        # get all locations from the database
        db_locations = self.current_session.query(self.tables.location).all()
        # extract their identifiers
        db_ids = [l.identifier for l in db_locations]
        # for each entry in exiobase, check if location code is in the identifier list
        matches = df_exiobase_locations.index.isin(db_ids)
        matched_locations = df_exiobase_locations[matches]
        unmatched_locations = df_exiobase_locations[~matches]

        return matched_locations, unmatched_locations, db_locations