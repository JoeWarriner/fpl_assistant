import schema.define_schema as define_schema
import seed_db.seed as seed_db

define_schema.run()
seed_db.run()