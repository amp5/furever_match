## **1/22/20**
API call was not efficient. refactored call by first calling API to identify zipcodes shelters are in for WA state. Then from that zipcode I call the API to find all adoptable cats within those zipcodes. 

## **1/17/20**

Working on acquiring basic data sets. 
First one is finding list of zip codes so I can filter out only Pacific Northwest. 
Found zipcode database here https://www.unitedstateszipcodes.org/zip-code-database/

Zip codes within PNW only are "pnw_zip codes.csv"


**To Scale** Start adding more states. Maybe move onto CA and then the rest of the West coast?

During first day of collecting data from API have 14.6MB of data (not including data retreived but not saved)
