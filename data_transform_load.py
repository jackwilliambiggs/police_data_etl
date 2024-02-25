import pandas as pd
from datetime import datetime
import os
import json
import psycopg2
from dotenv import load_dotenv


# Reading in the latest file that has been created that contains "raw" as 
directory = os.getcwd()

# Filter files containing "raw" in their name
raw_files = [file for file in os.listdir(directory) if "raw" in file]

# Get the latest CSV file based on modification time
latest_file = max(raw_files, key=lambda x: os.path.getmtime(os.path.join(directory, x)))
police_df = pd.read_csv(latest_file)

# Remove the unnamed column if it has been created
if 'Unnamed: 0' in police_df.columns:
    police_df.drop(columns=['Unnamed: 0'], inplace=True)
    print("Unnamed column exists and thus dropping")


# Dropping: Context and Outcome Status Columns
num_context_rows = police_df['context'].notnull().sum()

# Removing the context columns as seems another redundant variable. 
if num_context_rows > 0: 
    print(f"Number of rows with information in the 'context' column: {num_context_rows}.")
else: 
    print(f"There were no rows with information found in the 'context' column at all. For the purposes of this task I am going to remove the column")
    police_df.drop(columns=['context'], inplace=True)

# Function that will enable changing of the column names in the dataset
def replace_column_names(df):
    renamed_columns = {}
    for col in df.columns:
        new_col_name = col.replace(".", "_")
        if new_col_name != col:
            renamed_columns[col] = new_col_name
    return df.rename(columns=renamed_columns)


# Use function above to rename the column names more efficiently.
police_df = replace_column_names(police_df)

# Removing outcome_status as the api does not seem to provide any data in this column
police_df.drop(columns=['outcome_status'], inplace=True)

# There seem to be NaN in the outcome status category so this is some thing that would best be replaced with 'Status update unavailable'. 
police_df.fillna({'outcome_status_category': "Status update unavailable"}, inplace=True)

# Some observations have location subtype. Creating an indicator to enable easy filtering for later analysis. 
police_df['location_subtype_available'] = police_df['location_subtype'].notnull().astype(int)

# Add another indicator to enable easy filtering for whether there is a date available (there is a outcome)
police_df['outcome_status_date_available'] = police_df['outcome_status_date'].notnull().astype(int)

# Reading in JSON to create a *definte list* of categories for one hot encoding of categories for reproducibility 
file_name = "crime_categories.json"

# Open the JSON file and load the data
with open(file_name, "r") as file:
    crime_categories = json.load(file)

# extracting the possible categories from the json file.
possible_categories = [entry['url'] for entry in crime_categories]

# Create a new column with the category name - adding a 1 if obseration is found for the new  
for category in possible_categories:
    police_df[category] = (police_df['category'] == category).astype(int)


# Checking that this operation has worked properly. 
sum_of_categories = police_df[possible_categories].sum(axis=1)

# Check if any sum exceeds 1
if (sum_of_categories > 1).any():
    print("Warning: The sum of new columns exceeds 1 in some rows.")
else:
    print("All new rows have a valid sum of new columns after one hot encoding (<= 1).")

# Renaming columns again as the onehot encoding will have changed things
def replace_hyphens_with_underscores(df):
    """
    Replace hyphens "-" with underscores "_" in column names of a DataFrame.
    
    Parameters:
        df (pandas.DataFrame): Input DataFrame
    
    Returns:
        pandas.DataFrame: DataFrame with hyphens replaced by underscores in column names
    """
    # Create a dictionary to map old column names to new column names
    column_mapping = {col: col.replace('-', '_') for col in df.columns}
    
    # Rename columns using the mapping
    df = df.rename(columns=column_mapping)
    
    return df

police_df = replace_hyphens_with_underscores(police_df)

print(police_df.info())

current_date = datetime.now().strftime('%Y%m%d')

csv_name_and_path = current_date + "_tidy_police_data.csv"
police_df.to_csv(csv_name_and_path, index=False, encoding="utf-8")


# Preparing replacement values so I can create a table SQL
replacements ={
    'object': 'varchar', 
    'float64': 'float',
    'int64': 'int',
    'int32': 'int',
    'datetime64': 'timestamp', 
    'timedelta[ns]': 'varchar'
}

# Creating string to create table
col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(police_df.columns, police_df.dtypes.replace(replacements)))
print(col_str)

# Loading envirnmental variables for DB details. 
load_dotenv()

# Access environment variables
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_name = os.getenv("DB_name")
db_password = os.getenv("DB_PASSWORD")

conn_string = f"host={db_host} \
    dbname={db_name} \
    user={db_user} \
    password={db_password}"

# Establish connection to database
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# drop tables with the same name. 
cursor.execute("drop table if exists police_data")

# Create the table 
cursor.execute(f"CREATE TABLE police_data ({col_str})")


# inserting data to the table 
# Directory containing tidy data files
tidy_data_directory = "tidy"

# Get list of all CSV files in the directory
tidy_files = [file for file in os.listdir(directory) if "tidy" in file]

# Get the latest CSV file based on modification time
latest_file = max(tidy_files, key=lambda x: os.path.getmtime(os.path.join(directory, x)))
file_to_upload = open(latest_file)

SQL_STATEMENT = """
    COPY police_data FROM STDIN WITH
    CSV
    HEADER
    DELIMITER AS ','
    """

cursor.copy_expert(sql=SQL_STATEMENT, file = file_to_upload)
print("File copied to db")

cursor.execute("grant select on table police_data to public")
conn.commit()
cursor.close()
print("table police_data imported to db completed.")








