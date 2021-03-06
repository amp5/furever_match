import petpy
import json
import boto3
from datetime import date

s3 = boto3.resource('s3')

# API
PETFINDER_KEY=[KEY]
PETFINDER_SECRET=[SECRET]


pf = petpy.Petfinder(key=PETFINDER_KEY, secret=PETFINDER_SECRET)
today = date.today()

state_list = ['WA', 'OR', 'CA', 'TX', 'NV']

all_orgs_df = pf.organizations(state=state_list, return_df = True)

corrupted_zips = ['89447', '89429', '89019', '97138']

###### Animal Info ##########
shelter_zipcodes = []
for org in all_orgs_df:
    if all_orgs_df['address.postcode'] not in shelter_zipcodes:
        shelter_zipcodes.append(all_orgs_df['address.postcode'])

# kirkland 98033 zip code is ~3 mile radius
# Did preliminary search for all available pets in both Seattle and Portland and max pages was 22
for zipcode in shelter_zipcodes[0]:
    if zipcode not in corrupted_zips:
        cats_df = pf.animals(animal_type='cat', location=zipcode, distance=10,
                         results_per_page=50, pages=30)
        filename = str(today) + '_' + str(zipcode) + '.json'
        s3object = s3.Object('fureverdump', filename)
        s3object.put(Body=(bytes(json.dumps(cats_df).encode('UTF-8'))))
