params = {
    "DATA_FOLDER": "/opt/airflow/data",
    "DB_ENGINE": "postgresql+psycopg2://airflow:airflow@postgres/airflow",
    "DB_SCHEMA": "public",
    "POSTAL_LOCATIONS_URL": "https://www.serviceobjects.com/public/zipcode/ZipCodeFiles.zip",
    "POSTAL_LOCATIONS_FILE_NAME": "CanadianPostalCodes202312.csv",
    "POSTAL_LOCATIONS_FILE_TAG": "postal-locations",
    "PROPERTY_SALES_URL": "https://toronto-property-sales-kaggle.s3.us-west-2.amazonaws.com/properties.zip",
    "PROPERTY_SALES_FILE_NAME": "properties.csv",
    "PROPERTY_SALES_FILE_TAG": "properties",
    "RECREATION_FACILITIES_URL": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/cbea3a67-9168-4c6d-8186-16ac1a795b5b/resource/fd8ef4fb-2163-4c36-8530-e7252119dd59/download/facilities-data.xml",
    "RECREATION_FACILITIES_FILE_TAG": "recreation-faciltiies",
}