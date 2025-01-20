# Deployment Walkthrough
This end-to-end demo application utilizes a Fabric Notebook. The underlying technology is python along with our public apis. Below you'll find all the relavant details on what was used and why!

## Import required Libraries
There are a few things we need to import here
- The easiest way to call Microsoft Fabric Public APIs is to utilize the sempy.fabric library
- For any of the API with Definition calls we need to work with base64  strings. In order to decode and encode thse we'll import the base64 library
- When we create the names for the items in fabric we utilize the uuid library to generate random ids
- We need to read and update json which we use the json library

```
!pip install semantic-link --q

import sempy.fabric as fabric
import base64
import time
import uuid
import json
```

Next we setup all the variables that will be needed for the deployment
- client: used to call the fabric api
- workspaceId: populated with the workspace id that the notebook is in
- randomId: This is a random guid that will be appended to all our items to make sure the name is unique
- EventhouseName: name of the Eventhouse item that is created
- EventstreamName: name of the Eventstream item that is created
- DataActivatorName: anme of the Data Activator item thta is created
- DBName: name of the KQL Database that will be created on the Eventhouse
- DBCache: database level setting for the days we'll keep data in hot cache on the KQL Database
- DBStorage: database level setting for the days we'll retain the data on the KQL Database

```
client = fabric.FabricRestClient()
workspaceId = fabric.get_workspace_id()
randomId=uuid.uuid4()
EventhouseName = f"{'SampleEventhouse'}_{randomId}"
EventstreamName = f"{'SampleEventstream'}_{randomId}"
DataActivatorName = f"{'SampleDataActivator'}_{randomId}"
DBName=f"{'TaxiDB'}_{randomId}"
DBCache="P30D"
DBStorage="P365D"
```

First we'll create the Data Activator item and grab the item id to be used later using a varialbe called DataActivatorId.

Here is the documentation for this API

[Data Activator Create Item](https://learn.microsoft.com/en-us/rest/api/fabric/reflex/items/create-reflex?tabs=HTTP)

```
url = f"v1/workspaces/{workspaceId}/reflexes"

payload = {
    "displayName": f"{DataActivatorName}"
}

response=client.post(url,json=payload)

DataActivatorId=response.json()['id']
```

Next we'll create the Eventhouse Item and grab the item id to be used later using the variable called EventhouseId.

Here is the documentation for this API

[Eventhouse Create Item](https://learn.microsoft.com/en-us/rest/api/fabric/eventhouse/items/create-eventhouse?tabs=HTTP)

```
url = f"v1/workspaces/{workspaceId}/eventhouses"

payload = {
    "displayName": f"{EventhouseName}"
}

response=client.post(url,json=payload)

EventhouseId=response.json()['id']
```

Wen you create an Eventhouse you will get a default database with the same name as the Eventhouse. In this case we'll create a new database so the below code deletes the default database.

Here is the documentation for this API
[KQL Database Delete Item](https://learn.microsoft.com/en-us/rest/api/fabric/kqldatabase/items/delete-kql-database?tabs=HTTP)

```
url = f"v1/workspaces/{workspaceId}/kqlDatabases"

response=client.get(url)

print(response.json())

for item in response.json()['value']:
    if item['displayName'] == f"{EventhouseName}":
        url = f"v1/workspaces/{workspaceId}/kqlDatabases/{item['id']}"
        client.delete(url)
```

Now we need to create the new KQL Database and at the same time configure the database using a command script. The below needs to accomplished using the commands
- Create the TaxiRaw table
- Disable streaming ingestion on TaxiRaw table
- Modify the caching and retention policy on this table
- Create the ZoneLookup table
- Create a function to be used in the update policy
- Create the TaxiRecords table
- Add the update policy to the TaxiRecords table
- Modify the caching and retention policy on this table
- Create the TaxiRecordsDedup materialized view
- Modify the caching and retention policy on this materialized view
- Create the TaxiRecordsHourly materialized view
- Modify the caching and retention policy on this materialized view

This can be accomplished using the create with definition API.

Here is the documentation for this API
[KQL Database Create with Definition](https://learn.microsoft.com/en-us/rest/api/fabric/kqldatabase/items/create-kql-database?tabs=HTTP)

More information about the payload can be found here
[KQL Database Definition](https://learn.microsoft.com/en-us/rest/api/fabric/articles/item-management/definitions/kql-database-definition)

In short there are two section of this definition
- Platform: Here we say which Eventhouse this will be created on along with setting the database level caching and retention policies
- Definition: This is where we configure the database by running KQL commands

For both of these parts we need to create base64 strings. So that is the first thing we do.
```
url = f"v1/workspaces/{workspaceId}/kqlDatabases"

dbproperties={
  "databaseType": "ReadWrite",
  "parentEventhouseItemId": f"{EventhouseId}", 
  "oneLakeCachingPeriod": f"{DBCache}", 
  "oneLakeStandardStoragePeriod": f"{DBStorage}" 
}

dbproperties = json.dumps(dbproperties)


dbschema=""".create-merge table TaxiRaw (VendorID:string, tpep_pickup_datetime:datetime, tpep_dropoff_datetime:datetime, passenger_count:real, trip_distance:real, RatecodeID:real, store_and_fwd_flag:string, PULocationID:string, DOLocationID:string, payment_type:long, fare_amount:real, extra:real, mta_tax:real, tip_amount:real, tolls_amount:real, improvement_surcharge:real, total_amount:real, congestion_surcharge:real, airport_fee:real) with (folder = "", docstring = "")  
.alter table TaxiRaw policy streamingingestion disable 
.alter table TaxiRaw policy retention @'{"SoftDeletePeriod":"1.00:00:00","Recoverability":"Enabled"}'
.alter table TaxiRaw policy caching hot = 1d
.create-or-alter table TaxiRaw ingestion json mapping 'TaxiRaw_mapping' ```[{"column":"VendorID","path":"$['VendorID']","datatype":""},{"column":"tpep_pickup_datetime","path":"$['tpep_pickup_datetime']","datatype":""},{"column":"tpep_dropoff_datetime","path":"$['tpep_dropoff_datetime']","datatype":""},{"column":"passenger_count","path":"$['passenger_count']","datatype":""},{"column":"trip_distance","path":"$['trip_distance']","datatype":""},{"column":"RatecodeID","path":"$['RatecodeID']","datatype":""},{"column":"store_and_fwd_flag","path":"$['store_and_fwd_flag']","datatype":""},{"column":"PULocationID","path":"$['PULocationID']","datatype":""},{"column":"DOLocationID","path":"$['DOLocationID']","datatype":""},{"column":"payment_type","path":"$['payment_type']","datatype":""},{"column":"fare_amount","path":"$['fare_amount']","datatype":""},{"column":"extra","path":"$['extra']","datatype":""},{"column":"mta_tax","path":"$['mta_tax']","datatype":""},{"column":"tip_amount","path":"$['tip_amount']","datatype":""},{"column":"tolls_amount","path":"$['tolls_amount']","datatype":""},{"column":"improvement_surcharge","path":"$['improvement_surcharge']","datatype":""},{"column":"total_amount","path":"$['total_amount']","datatype":""},{"column":"congestion_surcharge","path":"$['congestion_surcharge']","datatype":""},{"column":"airport_fee","path":"$['airport_fee']","datatype":""}]```
.create-merge table ZoneLookup (LocationID:string,	Borough:string,	Zone:string,	service_zone:string)
.create-or-alter function with(skipvalidation=true) TaxiUpdate() {TaxiRaw | lookup (ZoneLookup | project DOLocationID=LocationID, DOBourough=Borough, DOZone=Zone, DOService_Zone=service_zone) on DOLocationID | lookup (ZoneLookup | project PULocationID=LocationID, PUBourough=Borough, PUZone=Zone, PUService_Zone=service_zone) on PULocationID }
.create-merge table TaxiRecords (VendorID:string, tpep_pickup_datetime:datetime, tpep_dropoff_datetime:datetime, passenger_count:real, trip_distance:real, RatecodeID:real, store_and_fwd_flag:string, PULocationID:string, DOLocationID:string, payment_type:long, fare_amount:real, extra:real, mta_tax:real, tip_amount:real, tolls_amount:real, improvement_surcharge:real, total_amount:real, congestion_surcharge:real, airport_fee:real, DOBourough:string, DOZone:string, DOService_Zone:string, PUBourough:string, PUZone:string, PUService_Zone:string) 
.alter table TaxiRecords policy update ```[{"IsEnabled": true, "Source": "TaxiRaw", "Query": "TaxiUpdate()", "IsTransactional": true, "PropagateIngestionProperties": false }]```
.alter table TaxiRecords policy retention @'{"SoftDeletePeriod":"10.00:00:00","Recoverability":"Enabled"}'
.alter table TaxiRecords policy caching hot = 3d
.create-or-alter materialized-view TaxiRecordsDedup on table TaxiRecords {TaxiRecords | summarize take_any(*) by VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, trip_distance }
.alter materialized-view  TaxiRecordsDedup policy retention @'{"SoftDeletePeriod":"30.00:00:00","Recoverability":"Enabled"}'
.alter materialized-view TaxiRecordsDedup policy caching hot = 14d
.create-or-alter materialized-view TaxiRecordsHourly on materialized-view TaxiRecordsDedup {TaxiRecordsDedup | summarize Avg_Passenger_Count=avg(passenger_count), Avg_Trip_Distance=avg(trip_distance), Avg_Fare_Amount=avg(fare_amount), Avg_Extra=avg(extra), Avg_MTA_Tax=avg(mta_tax), Avg_Tip_Amount=avg(tip_amount), Avg_Tolls_Amount=avg(tolls_amount), Avg_Improvement_Surcharge=avg(improvement_surcharge), Avg_Total_Amount=avg(total_amount), Avg_Congestion_Surcharge=avg(congestion_surcharge), Avg_Airport_Fee=avg(airport_fee) by bin(tpep_pickup_datetime,1h), bin(tpep_dropoff_datetime,1h), store_and_fwd_flag, PULocationID, DOLocationID, payment_type, DOBourough, DOZone, DOService_Zone, PUBourough, PUZone, PUService_Zone}
.alter materialized-view  TaxiRecordsHourly policy retention @'{"SoftDeletePeriod":"365.00:00:00","Recoverability":"Enabled"}'
.alter materialized-view TaxiRecordsHourly policy caching hot = 60d
"""

dbproperties_string = dbproperties.encode('utf-8')
dbproperties_bytes = base64.b64encode(dbproperties_string)
dbproperties_string = dbproperties_bytes.decode('utf-8')

dbschema_string = dbschema.encode('utf-8')
dbschema_bytes = base64.b64encode(dbschema_string)
dbschema_string = dbschema_bytes.decode('utf-8')
```

At the end of this we have a base64 string called dbproperties_string for the platform section and dbschema_string for the definition part. Both in base64 format. Now we can call the create with definition to create and configure the new database.
```
url = f"v1/workspaces/{workspaceId}/kqlDatabases"

payload = {
    "displayName": f"{DBName}",
    "definition": {
      "parts": [
        {
          "path": "DatabaseProperties.json",
          "payload": f"{dbproperties_string}",
          "payloadType": "InlineBase64"
        },
        {
          "path": "DatabaseSchema.kql",
          "payload": f"{dbschema_string}",
          "payloadType": "InlineBase64"
        }
      ]
  }
}

response=client.post(url,json=payload)
```

Because this is a long running operation and we cannot continue with our deployment until we verify sucess we have some code to wait for completion. At the end we poplulate a varialbe called dbId with the id of the newly created and configured database.
```
print(f"Create request status code {response.status_code}")
print(response.headers['Location'])
async_result_polling_url = response.headers['Location']

while True:
    async_response = client.get(async_result_polling_url)
    async_status = async_response.json().get('status').lower()
    print("Long running operation status " + async_status)
    if async_status != 'running':
        break
   
    time.sleep(3)

print("Long running operation reached terminal state '" + async_status +"'")

if async_status == 'succeeded':
    print("The operation completed successfully.")
    final_result_url= async_response.headers['Location']
    final_result = client.get(final_result_url)
    print(f"Final result: {final_result.json()}")
elif async_status == 'failed':
    print("The operation failed.")
else:
    print("The operation is in an unexpected state:", status)

dbId=final_result.json()['id']
```
Before we start ingesting the raw records we need to poplulate the ZoneLookup table. The data needed for that table is sitting on blob storage so we can use our .ingest command to populate that table.
```
import requests
url = f"v1/workspaces/{workspaceId}/eventhouses/{EventhouseId}"

response=client.get(url)

queryURI=response.json()['properties']['queryServiceUri']
HostName=queryURI.replace("https://", "")

url = f"{queryURI}/v1/rest/mgmt"
token_string = mssparkutils.credentials.getToken(f"{queryURI}")
header = {'Content-Type':'application/json','Authorization': f'Bearer {token_string}'}

payload = {
    "csl": ".ingest into table ZoneLookup (h'https://bwattspremium.blob.core.windows.net/taxi/Reference/taxi_zone_lookup.csv?sp=r&st=2025-01-10T00:44:17Z&se=2029-10-31T07:44:17Z&spr=https&sv=2022-11-02&sr=b&sig=FfPTKPHiAxQvET6yXS%2BCg6WB6aPlrhElCqJO8AWVF4o%3D') with (ignoreFirstRecord = true)",
    "db": f"{DBName}"
}

response=requests.post(url,json=payload, headers=header)
```
Everything is now setup and ready to receive data from Eventstream. What we'll configure in Eventstream is the following
- Utilize the built-in datasource for NYC Taxi
- Use the Eventprocessor to make sure all the fields are in the correct data type
- Output the results to our TaxiRaw table in Eventhouse
- Output the results to the Data Activator item. This does create alerts but sets up DA for you to easily configure some test alerts if you like.

We can use the create item with definition in order to do this.

Here is the documentation for Eventstream with definition
[Eventstream Create Item](https://learn.microsoft.com/en-us/rest/api/fabric/eventstream/items/create-eventstream?tabs=HTTP)
[Evenstream Definition](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/api-create-with-definition)


