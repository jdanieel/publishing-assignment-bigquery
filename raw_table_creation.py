from google.cloud import bigquery
import google.auth
from google.oauth2 import service_account
import json

credentials = service_account.Credentials.from_service_account_file(
    'sa_credentials.json')
project = 'publishing-assignment'

scoped_credentials = credentials.with_scopes(
    [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ])

client = bigquery.Client(credentials=scoped_credentials, project=project)
## Set dataset_id to the ID of the dataset to fetch.
dataset_id = "publishing-assignment.raw__acuity"

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
try:
  dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
except google.api_core.exceptions.Conflict:
  print("Dataset {} already created on project {}".format(dataset.dataset_id,client.project))
  pass

# Configure the external data source.
dataset = client.get_dataset(dataset_id)


with open('table_schema.json') as json_schema:
    schema_list = json.load(json_schema)

schema = []

for field in schema_list:
  schema.append(bigquery.SchemaField(
      name=field["name"], 
      mode=field["mode"],
      field_type=field["type"], 
      description=field["description"]
      ))

table_id = "appointments"
table = bigquery.Table(dataset.table(table_id), schema=schema)
table = client.create_table(table)  # Make an API request.
