# Importing relevant dependencies
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

# Postcode for the API that I would like to look at.
postcode = "E14 4PU"

# Getting the longitude and latitude from the postcodes.io
url = "https://api.postcodes.io/postcodes/" + postcode

# Get the data from the service and convert to json
response = requests.get(url).json()

latitude = response["result"]["latitude"]
longitude = response["result"]["longitude"]


def generate_month_year_list(start_date, end_date):
    # Parse start and end dates
    start_year, start_month = map(int, start_date.split('-'))
    end_year, end_month = map(int, end_date.split('-'))

    # Initialize a list to store month-year strings
    month_year_list = []

    # Start from the start date
    current_date = datetime(start_year, start_month, 1)

    # Loop until the end date is reached
    while current_date <= datetime(end_year, end_month, 1):
        # Add current month-year string to the list
        month_year_list.append(current_date.strftime('%Y-%m'))

        # Move to the next month
        current_date += relativedelta(months=1)

    return month_year_list


def get_data_for_month(month_year):
    # Make a request to the API for the given month
    url = f"https://data.police.uk/api/crimes-street/all-crime?lat={str(latitude)}&lng={str(longitude)}&date={month_year}"
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {month_year}. Status code: {response.status_code}")
        return None
    
# Defining date range to collect data from.
start_date = "2022-01"
end_date = "2022-06"

# Generate list of month-year strings

months_to_hit_api = generate_month_year_list(start_date, end_date)

# List to store DataFrames for each month
dfs = []

# Loop over each month
for month_year in months_to_hit_api:
    # Retrieve data for the month
    data = get_data_for_month(month_year)
    
    # Convert data to DataFrame
    if data:
        df = pd.json_normalize(data)
        
        # Append DataFrame to the list
        dfs.append(df)

# Concatenate DataFrames into a single DataFrame
police_data = pd.concat(dfs, ignore_index=True)

current_date = datetime.now().strftime('%Y%m%d')

csv_name_and_path = current_date + "-raw_police_data.csv"
police_data.to_csv(csv_name_and_path, index=False, encoding = "utf-8")


# Getting the list of all categories

url = "https://data.police.uk/api/crime-categories"
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Save the JSON response to a file
    with open("crime_categories.json", "w") as file:
        json.dump(response.json(), file)
    print("JSON response saved successfully.")
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")