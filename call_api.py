import requests
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
from process_json import fetch_data, append_null, total_keys
import pandas as pd
import time


load_dotenv(find_dotenv())

access_token = os.environ.get("access_token")


url = 'https://globalmart-api.onrender.com/mentorskool/v1/sales'
headers = {"access_token": access_token}


total_records = 0
# response_limit = 100
start_time = time.time()
counter = 1
final_data = []
offset = 1
while offset<3000:
    print(f"This is {counter} iteration")

    try:
        response = requests.get(url + f'?offset={offset}&limit=100', headers=headers)
        response.raise_for_status()
        response_json = response.json()
    except requests.exceptions.HTTPError as err:
        # Need to check its an 404, 503, 500, 403 etc.
        status_code = err.response.status_code
        if status_code==403:
            print("Your request is not authenticated, thus please pass the headers properly...")
        elif status_code==404:
            print("Please check the url properly..")
        
        ## we can add multiple if else blocks here..

    data = response_json["data"]
    next_cursor = response_json.get("next")
    
    ## Append the data to the final_data
    final_data = final_data + data

    ## Now reset the offset value, if next cursor is not null
    if next_cursor is None or next_cursor=="":
        break
    offset+=100
    ## Increase the total_records
    total_records+=response_json["limit"]
    print(total_records)

    ## This code is inefficient as only 5 counter takes 37 seconds, thus 100 counter that is 20 times more than 5
    ## will take 37*20 = 740 seconds i.e near about 12.33 minutes
    # if counter==5:
    #     break
    counter+=1

## Now as the data is fetched, process the data
## 1. Find the maximum keys that are available in the response
# ## find the total_keys
max_keys = set() ## create an empty set

## Now iterate through the whole data and fetch all the keys and store it into a set
for single_data in data:
    return_keys = total_keys(single_data, max_keys) ## the function will take the empty set and will fill that by keys
    max_keys = max_keys.union(return_keys)

## Now create an empty dictionary with all the keys
final_data = {key: [] for key in max_keys}

## now traverse through the data and extract the data
for single_data in tqdm(data):
    final_data = fetch_data(single_data, final_data)

    ## Check if there is any list that contains less value means there are some attribute missing in one dict which is 
    # there in another one
    length = [len(value) for value in final_data.values()]

    ## Now convert the list into set, to see find the unique values
    unique_length =  set(length)
    
    ## Now if unique length is greater than 1, means there is some attribute that are missing in this dict and thus, we have to search
    ## for that in the fetched_data and append null at the end.
    if len(unique_length) > 1:
        ## find the max value of list and check if there is any list that contains value less than the max, then append null
        ## at the end of that
        max_len = max(length)
        final_data = append_null(final_data, max_len)


transaction_df = pd.DataFrame(final_data)
## there are null values returned by API, thus replace null with None in pandas
transaction_df2 = transaction_df.replace("null", None)
print(transaction_df.head())

# end_time = time.time()
# print(f'The program run for {end_time - start_time} seconds')
