from Database import Database
from mrio_common_metadata.conversion.exiobase_3_hybrid_io import convert_exiobase
from mrio_common_metadata.utils import load_compressed_csv, load_compressed_csv_as_dataframe
import pathlib
import json



exiobase_dir = pathlib.Path(r"S:\Benjamin Portner\docs\databases\EXIOBASE_3.3.17_hsut_2011")

# first, convert exiobase data using Chris Mutel's mrio_common_metadata tool
# NOTE: THIS TAKES LONG (on the order of one hour)
# convert_exiobase(exiobase_dir, version="3.3.17 hybrid")

converted_dir = pathlib.Path(r".\exiobase")

# read json file with metadata about exiobase and the converted packages
file = pathlib.Path(converted_dir / "datapackage.json")
exiobase_metadata = json.load(open(file, "r"))

# connect to the database
db = Database(username = "bonsai")
session = db.current_session
tables = db.tables

# check which exiobase locations are not yet contained in the database
exiobase_location_metadata = [r for r in exiobase_metadata["resources"] if r["name"] == "locations"][0]
file = pathlib.Path(converted_dir / exiobase_location_metadata["path"])
df_exiobase_locations = load_compressed_csv_as_dataframe(file, exiobase_metadata)
matched_locations, unmatched_locations, db_locations = db.match_exiobase_locations(df_exiobase_locations)

# add unmatched locations
for i, d in unmatched_locations.reset_index().iterrows():
    location_data = dict(
        identifier = d["code"],
        label = d["name"],
        #uri = None,
    )
    db.add_entry(tables.location, location_data)
    pass

# problem with 'Wallis and Futuna Is.'

# find the exiobase license among existing entries in the database
exiobase_license = session.query(tables.license).filter(
    tables.license.full_name.ilike("%creative commons attribution share alike 4.0%")
)[0]

# convert the "exiobase source" description (dictionary) to a string
exiobase_source = exiobase_metadata["sources"][0].__str__().replace('\'', '').replace('}', '').replace('{', '')

# add exiobase to the database as "datasource"
source_data = dict(
    label =exiobase_metadata["name"] + " converted using mrio_common_metadata",
    license = exiobase_license,
    license_id = exiobase_license.id,
    #location = "",
    #location_id = "",
    source = exiobase_source,
    version = exiobase_metadata["version"],
    description = exiobase_metadata["description"],
)
source_entry = db.add_entry(tables.datasource, source_data)

# add two dummy agents
agent_entries = []
for i in range(1):
    agent_data = dict(
        label = "dummy agent "+str(i),
        #location = "",
        #location_id = "",
        datasource = source_entry,
        datasource_id = source_entry.id,
    )
    agent_entry = db.add_entry(tables.agent, agent_data)
    agent_entries.append(agent_entry)


pass



