from Database import start_session
from mrio_common_metadata.conversion.exiobase_3_hybrid_io import convert_exiobase
from mrio_common_metadata.utils import load_compressed_csv, load_compressed_csv_as_dataframe
import pathlib
import json


# helper function to check which exiobase locations are already contained in the database
# right now rather stupid (check by matching database identifiers with exiobase country codes)
def match_exiobase_bonsai_locations(session, df_exiobase_locations):
    # get all locations from the database
    db_locations = session.query(tables.location).all()
    # extract their identifiers
    db_ids = [l.identifier for l in db_locations]
    # for each entry in exiobase, check if location code is in the identifier list
    matches = df_exiobase_locations.index.isin(db_ids)
    matched_locations = df_exiobase_locations[matches]
    unmatched_locations = df_exiobase_locations[~matches]

    return matched_locations, unmatched_locations, db_locations



exiobase_dir = pathlib.Path(r"S:\Benjamin Portner\docs\databases\EXIOBASE_3.3.17_hsut_2011")

# first, convert exiobase data using Chris Mutel's mrio_common_metadata tool
# NOTE: THIS TAKES LONG (on the order of one hour)
# convert_exiobase(exiobase_dir, version="3.3.17 hybrid")

converted_dir = pathlib.Path(r".\exiobase")

# read json file with metadata about exiobase and the converted packages
file = pathlib.Path(converted_dir / "datapackage.json")
exiobase_metadata = json.load(open(file, "r"))

# connect to the database
session, base_class, tables, db_meta_data, db = start_session()


# check which exiobase locations are not yet contained in the database
table_metadata = [r for r in exiobase_metadata["resources"] if r["name"] == "locations"][0]
file = pathlib.Path(converted_dir / table_metadata["path"])
df_exiobase_locations = load_compressed_csv_as_dataframe(file, exiobase_metadata)
matched_locations, unmatched_locations, db_locations = match_exiobase_bonsai_locations(session, df_exiobase_locations)

# problem with 'Wallis and Futuna Is.'

# find the exiobase license among existing entries in the database
exiobase_license = session.query(tables.license).filter(
    tables.license.full_name.ilike("%creative commons attribution share alike 4.0%")
)[0]

# convert the "exiobase source" description (dictionary) to a string
exiobase_source = exiobase_metadata["sources"][0].__str__().replace('\'', '').replace('}', '').replace('{', '')

# add exiobase to the database as "datasource"
number_sources_in_db = session.query(tables.datasource).count()
source = tables.datasource(
    id = number_sources_in_db+1,
    label =exiobase_metadata["name"] + " converted using mrio_common_metadata",
    license = exiobase_license,
    license_id = exiobase_license.id,
    #location = "",
    #location_id = "",
    source = exiobase_source,
    version = exiobase_metadata["version"],
    description = exiobase_metadata["description"],
)

# add exiobase contributors to the database as "agents"
number_agents_in_db = session.query(tables.agent).count()
for a in exiobase_metadata["contributors"]:
    agent = tables.agent(
        id = number_agents_in_db+1,
        label = a["title"],
        #location = "",
        #location_id = "",
        datasource = source,
        datasource_id = source.id,
    )
    number_agents_in_db += 1



