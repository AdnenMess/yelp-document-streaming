import streamlit as st
import requests
from pandas import DataFrame

# Define the base URL of the API
API_BASE_URL = "http://api-output:8084"

# Below the first chart add an input field for the invoice number
bus_id = st.sidebar.text_input("Business ID:")

# if enter has been used on the input field
if bus_id:
    # Make a GET request to the API endpoint with the invoice ID
    response = requests.get(f"{API_BASE_URL}/yelp/business/{bus_id}")
    # st.text(response.text)

    if response.ok:
        # Create the dataframe from the response data
        data = response.json()
        df = DataFrame([data], index=range(1))
        # st.write(data)

        # Reindex the dataframe to order columns lexicographically
        re_indexed = df.reindex(sorted(df.columns), axis=1)

        # Add the table with a headline
        st.header("Output Business ID")
        table2 = st.dataframe(data=re_indexed)
    else:
        st.error("Failed to retrieve Business ID data")

usr_id = st.sidebar.text_input("User ID:")

# if enter has been used on the input field
if usr_id:
    # Make a GET request to the API endpoint with the invoice ID
    response = requests.get(f"{API_BASE_URL}/yelp/user/{usr_id}")
    # st.text(response.text)

    if response.ok:
        # Create the dataframe from the response data
        data = response.json()
        df = DataFrame([data], index=range(1))
        # st.write(data)

        # Reindex the dataframe to order columns lexicographically
        re_indexed = df.reindex(sorted(df.columns), axis=1)

        # Add the table with a headline
        st.header("Output User ID")
        table2 = st.dataframe(data=re_indexed)
    else:
        st.error("Failed to retrieve User ID data")

