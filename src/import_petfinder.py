import petpy
import json
import boto3
from datetime import date

s3 = boto3.resource('s3')

PETFINDER_KEY='FGqq3oNQHnOIF55N8Y947T83vEa3R7WvXzZUpJDrJjw5FuW1dr'
PETFINDER_SECRET='HK9loGC6r1FoAlqiKnJmght8UafzgGZcoL533eWM'


pf = petpy.Petfinder(key=PETFINDER_KEY, secret=PETFINDER_SECRET)
today = date.today()

all_orgs_df = pf.organizations(state=['WA', 'OR', 'CA', 'NV', 'AZ'], return_df = True)
shelter_zipcodes = []
for org in all_orgs_df:impo
    if all_orgs_df['address.postcode'] not in shelter_zipcodes:
        shelter_zipcodes.append(all_orgs_df['address.postcode'])

# kirkland 98033 zip code is ~3 mile radius
# Did preliminary search for all available pets in both Seattle and Portland and max pages was 22
for zipcode in shelter_zipcodes[0]:
    cats_df = pf.animals(animal_type='cat', location=zipcode, distance=10,
                     results_per_page=50, pages=30)
    filename = str(today) + '_' + str(zipcode) + '.json'
    s3object = s3.Object('fureverdump', filename)
    s3object.put(Body=(bytes(json.dumps(cats_df).encode('UTF-8'))))

