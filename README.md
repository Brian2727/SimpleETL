# ETL Pipeline for Data Collection and Transformation
This project implements an ETL (Extract, Transform, Load) pipeline built in Python. The pipeline collects data from an external API, processes it, and stores the results for further analysis in both CSV format and an SQL database.

Key Features
Data Extraction: Uses the requests library to fetch data from an external API.
Data Validation: Performs input validation to ensure data quality, checking for missing fields and ensuring that titles or other key data points are correctly captured.
Data Transformation: A lightweight transformation process adds three additional columns to the dataset, converting key values into EUR, USD, GBP, and INR currencies.
Data Loading: The final dataset is saved both as a CSV file and loaded into a SQL database for further analysis.
Detailed Workflow
Data Extraction:
The pipeline utilizes the requests library to make API calls and retrieve the raw data. The data is then stored in a pandas DataFrame for easier manipulation and analysis.

Data Validation:
A series of input checks are performed to validate the integrity of the data. These checks ensure that:

There are no missing or null values in critical fields.
Titles and other key attributes are correctly populated.
Any invalid or unexpected data points are flagged for review.
Data Transformation:
A small transformation function is applied to enrich the data by adding three additional columns. These columns convert specific financial values into four currencies:

EUR (Euro)
USD (US Dollar)
GBP (British Pound)
INR (Indian Rupee)
Data Loading:
Once the data has been validated and transformed, it is saved to a CSV file for easy access. Additionally, the data is loaded into an SQL database for more complex analysis or integration into other systems.

Technologies Used
Python: The core programming language for the ETL pipeline.
requests: For making API requests and extracting data.
pandas: For data manipulation and transformation.
SQL: For structured data storage and querying.
CSV: To provide an additional, easily accessible format for data storage.
