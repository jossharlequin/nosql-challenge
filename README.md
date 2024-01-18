Eat Safe, Love
Part 1: Database and Jupyter Notebook Set Up
Import the data provided in the establishments.json file from your Terminal. Name the database uk_food and the collection establishments.

Within this markdown cell, copy the line of text you used to import the data from your Terminal. This way, future analysts will be able to repeat your process.

e.g.: Import the dataset with YOUR IMPORT TEXT HERE

#code to import json
mongoimport --type json -d uk_food -c establishments --drop --jsonArray establishments.json
#code to import json
mongoimport --type json -d uk_food -c establishments --drop --jsonArray establishments.json
# Import dependencies
from pymongo import MongoClient
from pprint import pprint
import numpy as np
# Create an instance of MongoClient
mongo = MongoClient(port=27017)
# confirm that our new database was created
print(mongo.list_database_names())
['admin', 'autosaurus', 'classDB', 'config', 'epa', 'fruits_db', 'gardenDB', 'local', 'met', 'petsitly_marketing', 'uk_food']
# assign the uk_food database to a variable name
db = mongo['uk_food']
# review the collections in our new database
print(db.list_collection_names())
['establishments']
# review the collections in our new database
print(db.list_collection_names())
['establishments']
# review a document in the establishments collection
pprint(db.establishments.find_one())

# assign the collection to a variable
establishments = db['establishments']
Part 2: Update the Database
An exciting new halal restaurant just opened in Greenwich, but hasn't been rated yet. The magazine has asked you to include it in your analysis. Add the following restaurant "Penang Flavours" to the database.
# Create a dictionary for the new restaurant data
halal_data = {
    "BusinessName": "Penang Flavours",
    "BusinessType": "Restaurant/Cafe/Canteen",
    "BusinessTypeID": "",
    "AddressLine1": "Penang Flavours",
    "AddressLine2": "146A Plumstead Rd",
    "AddressLine3": "London",
    "AddressLine4": "",
    "PostCode": "SE18 7DY",
    "Phone": "",
    "LocalAuthorityCode": "511",
    "LocalAuthorityName": "Greenwich",
    "LocalAuthorityWebSite": "http://www.royalgreenwich.gov.uk",
    "LocalAuthorityEmailAddress": "health@royalgreenwich.gov.uk",
    "scores": {
        "Hygiene": "",
        "Structural": "",
        "ConfidenceInManagement": ""
    },
    "SchemeType": "FHRS",
    "geocode": {
        "longitude": "0.08384000",
        "latitude": "51.49014200"
    },
    "RightToReply": "",
    "Distance": 4623.9723280747176,
    "NewRatingPending": True
}
# Insert the new restaurant into the collection
db.establishments.insert_one(halal_data)
InsertOneResult(ObjectId('65a77478fe23e4e87d014768'), acknowledged=True)
result = db.establishments.find_one({ "BusinessName": "Penang Flavours" })
pprint(result)

Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the BusinessTypeID and BusinessType fields.
# Find the BusinessTypeID for "Restaurant/Cafe/Canteen" and return only the BusinessTypeID and BusinessType fields
result1 = db.establishments.find(
    { "BusinessType": "Restaurant/Cafe/Canteen" },
    { "_id": 0, "BusinessTypeID": 1, "BusinessType": 1 }
)
​
# Loop through the cursor and print each document
for document in result1:
    pprint(document)

**#Did this this to see all the business_type_ids in collection so I could see which one Penang should use**
unique_business_type_ids = db.establishments.distinct(
    "BusinessTypeID",
    { "BusinessType": "Restaurant/Cafe/Canteen" }
)

# Print the unique BusinessTypeIDs
for business_type_id in unique_business_type_ids:
    print("Unique BusinessTypeID:", business_type_id)

Update the new restaurant with the BusinessTypeID you found.
# Update the new restaurant with the correct BusinessTypeID
update_result = db.establishments.update_one(
    { "BusinessName": "Penang Flavours" },
    { "$set": { "BusinessTypeID": "1" } }
)
​
# Print the update result
print("Matched:", update_result.matched_count)
print("Modified:", update_result.modified_count)
Matched: 1
Modified: 1

# Confirm that the new restaurant was updated
result = db.establishments.find_one({ "BusinessName": "Penang Flavours" })
pprint(result)

The magazine is not interested in any establishments in Dover, so check how many documents contain the Dover Local Authority. Then, remove any establishments within the Dover Local Authority from the database, and check the number of documents to ensure they were deleted.
establishments.count_documents({})
39780
# Find how many documents have LocalAuthorityName as "Dover"
query = {'LocalAuthorityName': "Dover"}
​
# Print the number of results
print("Number of documents in result:", establishments.count_documents(query))
Number of documents in result: 994
# Delete all documents where LocalAuthorityName is "Dover"
delete_result = db.establishments.delete_many({ "LocalAuthorityName": "Dover" })
​
# Print the delete result
print("Deleted documents count:", delete_result.deleted_count)
Deleted documents count: 994
# Check if any remaining documents include Dover
query = {'LocalAuthorityName': "Dover"}
​
# Print the number of results
print("Number of documents in result:", establishments.count_documents(query))
Number of documents in result: 0
pprint(db.establishments.find_one())
# Check that other documents remain with 'find_one'
pprint(db.establishments.find_one())

Some of the number values are stored as strings, when they should be stored as numbers.
Use update_many to convert latitude and longitude to decimal numbers.

establishments.update_many({}, [ {'$set': { "geocode.latitude" : {'$toDouble': "$geocode.latitude"},
                                                "geocode.longitude" : {'$toDouble': "$geocode.longitude"}
                                              } 
                                     } ]
                              )
# Tried to do $toDecimal but I had conversion issues so I just did $toDouble
# Change the data type from String to Decimal for longitude and latitude
establishments.update_many({}, [ {'$set': { "geocode.latitude" : {'$toDouble': "$geocode.latitude"},
                                                "geocode.longitude" : {'$toDouble': "$geocode.longitude"}
                                              } 
                                     } ]
                              )
UpdateResult({'n': 38786, 'nModified': 38786, 'ok': 1.0, 'updatedExisting': True}, acknowledged=True)
# Got this from the class work
# Find documents with updated data types
updated_documents = db.establishments.find({
    'geocode.latitude': {'$exists': True},
    'geocode.longitude': {'$exists': True}
}).limit(10)
​
# Loop through the updated documents and print the latitude, longitude, and RatingValue values
for document in updated_documents:
    latitude_value = document['geocode']['latitude']
    longitude_value = document['geocode']['longitude']
​
    print(f"Document ID: {document['_id']}, Latitude Value: {latitude_value}, Longitude Value: {longitude_value}")

Use update_many to convert RatingValue to integer numbers.

unique_rating_values = db.establishments.distinct(
    "RatingValue")
​
# Did this so I could see all the values for RatingValues so I could do the null task
# Print the unique RatingValues
for unique_rating_value in unique_rating_values:
    print("RatingValue:", unique_rating_values)

RatingValue
# Set non 1-5 Rating Values to Null
result = establishments.update_many({}, [
    {
        '$set': {
            'RatingValue': {
                '$cond': {
                    'if': {'$in': ['$RatingValue', ['Pass', 'Fail', 'AwaitingInspection', 'Awaiting Inspection', 'AwaitingPublication', 'Exempt']]},
                    'then': None,
                    'else': {'$toInt': '$RatingValue'}
                }
            }
        }
    }
])
​
# Print the number of documents updated
print(f"Number of documents updated: {result.matched_count}")
Number of documents updated: 38786

**#Had to do this multiple times. This is from class assignment Air_Lift**
# Change the data type from String to Integer for RatingValue
establishments.update_many({}, [ {'$set': { "RatingValue" : {'$toInt': "$RatingValue"}
                                              } 
                                     } ]
                              )
UpdateResult({'n': 38786, 'nModified': 0, 'ok': 1.0, 'updatedExisting': True}, acknowledged=True)
# Check that the coordinates and rating value are now numbers
pprint(db.establishments.find_one())

**#Did this to varify the values were changed correctly**
# Sample of data
sample_document = db.establishments.find_one()
​
# Display the keys and data types of the sample document
for key, value in sample_document.items():
    print(f"Field: {key}, Data Type: {type(value)}")



Eat Safe, Love
Notebook Set Up
#code to import json
mongoimport --type json -d uk_food -c establishments --drop --jsonArray establishments.json
# Import dependencies
from pymongo import MongoClient
from pprint import pprint
import pandas as pd
import pymongo
# Create an instance of MongoClient
mongo = MongoClient(port=27017)
# assign the uk_food database to a variable name
db = mongo['uk_food']
# review the collections in our database
print(db.list_collection_names())
['establishments']
# assign the collection to a variable
establishments = db['establishments']
Part 3: Exploratory Analysis
Unless otherwise stated, for each question:

Use count_documents to display the number of documents contained in the result.
Display the first document in the results using pprint.
Convert the result to a Pandas DataFrame, print the number of rows in the DataFrame, and display the first 10 rows.
1. Which establishments have a hygiene score equal to 20?
{'scores.Hygiene': 20}
query = {'scores.Hygiene': 20}
result = db.establishments.find(query)
​
# Use count_documents to display the number of documents in the result
count_result = db.establishments.count_documents(query)
print(f"Number of documents with a hygiene score of 20: {count_result}")
​
**#I did this instead of find_one because I found it first when I was copy pasting**
# Display the first document in the results using pprint
for i in range (1):
    pprint(result[i])

# Convert the result to a Pandas DataFrame
result = db.establishments.find(query)
result_df = pd.DataFrame(result)
# Display the number of rows in the DataFrame
print("Rows in DataFrame: ", len(result_df))
​
# Display the first 10 rows of the DataFrame
result_df.head(10)
Rows in DataFrame:  41

2. Which establishments in London have a RatingValue greater than or equal to 4?
**#Did these next to figure out why I couldn't find 'London' in my original $regex attempts. I had to reupload my establiments.json because I had corrupted it**
unique_authorities = db.establishments.distinct('LocalAuthorityName')
print(unique_authorities)
['Aberdeenshire', 'Arun', 'Ashford', 'Babergh', 'Barking and Dagenham', 'Basildon', 'Bexley', 'Braintree', 'Brentwood', 'Bromley', 'Broxbourne', 'Canterbury City', 'Castle Point', 'Chelmsford', 'City of London Corporation', 'Colchester', 'Dartford', 'Dorset', 'East Hertfordshire', 'East Renfrewshire', 'East Suffolk', 'Eastbourne', 'Epping Forest', 'Folkestone and Hythe', 'Gravesham', 'Greenwich', 'Hackney', 'Harlow', 'Hastings', 'Havering', 'Ipswich', 'Kensington and Chelsea', 'Knowsley', 'Lambeth', 'Lewes', 'Lewisham', 'Maidstone', 'Maldon', 'Medway', 'Mid Sussex', 'Newham', 'North Hertfordshire', 'North Norfolk', 'Orkney Islands', 'Pendle', 'Reading', 'Redbridge', 'Rochford', 'Rother', 'Rushmoor', 'Salford', 'Sevenoaks', 'Slough', 'South Cambridgeshire', 'Southend-On-Sea', 'Spelthorne', 'Stratford-on-Avon', 'Sunderland', 'Swale', 'Tandridge', 'Tendring', 'Thanet', 'Thurrock', 'Tonbridge and Malling', 'Tower Hamlets', 'Tunbridge Wells', 'Uttlesford', 'Waltham Forest', 'Wealden', 'West Suffolk', 'York']
sample_documents = db.establishments.find({'LocalAuthorityName': {'$regex': 'London', '$options': 'i'}}).limit(5)
​
# Display a few sample documents
for document in sample_documents:
    pprint(document)

london_query = {
    'LocalAuthorityName': {'$regex': 'London', '$options': 'i'}
}
​
london_result = db.establishments.find(london_query)
​
# Display the results
for document in london_result:
    pprint(document)
**#this was to see if I was getting anything** 
match_query = {
    'LocalAuthorityName': {'$regex': 'London|City of London Corporation', '$options': 'i'},  # Case-insensitive match
    'RatingValue': {'$gte': 4}
}

result = db.establishments.find(match_query)

# Display the results
for document in result:
    pprint(document)

# Find the establishments with London as the Local Authority and has a RatingValue greater than or equal to 4.
match_query = {
    'LocalAuthorityName': {'$regex': 'London', '$options': 'i'},  # Case-insensitive match
    'RatingValue': {'$gte': 4}
}
​
# Count the documents
document_count = db.establishments.count_documents(match_query)
print(f"Number of documents: {document_count}")
​
# Find and display the first document
result = db.establishments.find(match_query)
​
# Display the first document using pprint
for i in range (1):
    pprint(result[i])

# Convert the result to a Pandas DataFrame
result_df = pd.DataFrame(result)

# Print out the length of the DataFrame
print("Rows in DataFrame: ", len(result_df))

# Display the first 10 rows of the DataFrame
result_df.head(10)
# Convert the result to a Pandas DataFrame
result_df = pd.DataFrame(result)
​
# Print out the length of the DataFrame
print("Rows in DataFrame: ", len(result_df))
​
# Display the first 10 rows of the DataFrame
result_df.head(10)

3. What are the top 5 establishments with a RatingValue rating value of 5, sorted by lowest hygiene score, nearest to the new restaurant added, "Penang Flavours"?
result = db.establishments.find_one({ "BusinessName": "Penang Flavours" })
pprint(result)

for i in range(5):
    pprint(result[i])

**#This was a combo of the aggregate class assignments but also from the one recently where we added the shipping cost. Here I am set the > < signs to limit the results.**
# Search within 0.01 degree on either side of the latitude and longitude.
# Rating value must equal 5
# Sort by hygiene score
degree_search = 0.01
latitude = 51.490142
longitude = 0.08384
​
query = {
    'geocode.latitude': {'$gte': latitude - degree_search, '$lte': latitude + degree_search},
    'geocode.longitude': {'$gte': longitude - degree_search, '$lte': longitude + degree_search},
    'RatingValue': 5
}
​
sort = [('scores.Hygiene', pymongo.DESCENDING)]
​
result = db.establishments.find(query).sort(sort)
​
# Print the results
for i in range(5):
    pprint(result[i])

result_df = pd.DataFrame(result)

# Print out the length of the DataFrame
print("Rows in DataFrame: ", len(result_df))

# Display the first 10 rows of the DataFrame
result_df.head(10)
# Convert result to Pandas DataFrame
result_df = pd.DataFrame(result)
​
# Print out the length of the DataFrame
print("Rows in DataFrame: ", len(result_df))
​
# Display the first 10 rows of the DataFrame
result_df.head(10)

4. How many establishments in each Local Authority area have a hygiene score of 0?
group_query = {'$group': {'LocalAuthorityName'}}
**#Got this from the pipeline lesson**
# Create a pipeline that: 
# 1. Matches establishments with a hygiene score of 0
match_query = {'$match': {'scores.Hygiene': 0}}
​
# 2. Groups the matches by Local Authority
group_query = {'$group': {'_id': '$LocalAuthorityName', 'count': {'$sum': 1}}}
​
# 3. Sorts the matches from highest to lowest
sort_values = {'$sort': { 'count': -1 }}
​
# Put the pipeline together
pipeline = [match_query, group_query, sort_values]
​
documents = list(establishments.aggregate(pipeline))
# Print the number of documents in the result
print("Number of documents in result: ", len(documents))
# Print the first 10 results
pprint(documents[0:10])
Number of documents in result:  55
[{'_id': 'Thanet', 'count': 1130},
 {'_id': 'Greenwich', 'count': 882},
 {'_id': 'Maidstone', 'count': 713},
 {'_id': 'Newham', 'count': 711},
 {'_id': 'Swale', 'count': 686},
 {'_id': 'Chelmsford', 'count': 680},
 {'_id': 'Medway', 'count': 672},
 {'_id': 'Bexley', 'count': 607},
 {'_id': 'Southend-On-Sea', 'count': 586},
 {'_id': 'Tendring', 'count': 542}]
# Convert the result to a Pandas DataFrame
documents_df = pd.DataFrame(documents)
​
# Print out the length of the DataFrame
print("Rows in DataFrame: ", len(documents_df))
​
# Display the first 10 rows of the DataFrame
documents_df.head(10)
Rows in DataFrame:  55
_id	count
0	Thanet	1130
1	Greenwich	882
2	Maidstone	713
3	Newham	711
4	Swale	686
5	Chelmsford	680
6	Medway	672
7	Bexley	607
8	Southend-On-Sea	586
9	Tendring	542
​


    
