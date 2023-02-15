import requests
from dotenv import load_dotenv, find_dotenv
import os
from tqdm import tqdm
from process_json import Data_Collection, Product, State, OrderDiscount
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
while total_records<500:
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
        else:
            print(err)
        break
        
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

    ## This code is inefficient as only 5 counter takes 37 seconds, thus 100 counter that is 20 times more than 5
    ## will take 37*20 = 740 seconds i.e near about 12.33 minutes
    # if counter==5:
    #     break
    counter+=1

## Create an instance for API_Collection
api_fetch = Data_Collection()

## Now as the data is fetched, process the data
## 1. Find the maximum keys that are available in the response
# ## find the total_keys
max_keys = set() ## create an empty set

## Now iterate through the whole data and fetch all the keys and store it into a set
for single_data in final_data:
    return_keys = api_fetch.total_keys(single_data, max_keys) ## the function will take the empty set and will fill that by keys
    max_keys = max_keys.union(return_keys)

## Now create an empty dictionary with all the keys
final_dict = {key: [] for key in max_keys}

## now traverse through the data and extract the data
for single_data in tqdm(final_data):
    final_dict = api_fetch.fetch_data(single_data, final_dict)

    ## Check if there is any list that contains less value means there are some attribute missing in one dict which is 
    # there in another one
    length = [len(value) for value in final_dict.values()]

    ## Now convert the list into set, to see find the unique values
    unique_length =  set(length)
    
    ## Now if unique length is greater than 1, means there is some attribute that are missing in this dict and thus, we have to search
    ## for that in the fetched_data and append null at the end.
    if len(unique_length) > 1:
        ## find the max value of list and check if there is any list that contains value less than the max, then append null
        ## at the end of that
        max_len = max(length)
        final_dict = Data_Collection.append_null(final_dict, max_len)


transaction_df = pd.DataFrame(final_dict)
## there are null values returned by API, thus replace null with None in pandas
transaction_df2 = transaction_df.replace("null", None)

## create new transaction
transaction_df2 = transaction_df.copy()
while True:
    choice = input("Do you want to find the product's sizes? Y: Yes, N: No:- ")
    if choice=="Y":
        product_name = input("Enter the product_name for which you have to find the sizes available:- ")
        ## An object of the prodcut is created, now using this object we can fetch the available sizes
        product = Product('Bush Somerset Collection Bookcase')
        # # Fetch the count of available sizes, the function will find the total sizes and will return the count of sizes
        total_sizes = product.product_size(transaction_df2)
        # print(total_sizes)
        ## To access the sizes there is an attribute called sizes
        print(f"The available sizes for the product `{product.product_name}` is:- ", product.sizes)
    
    elif choice == "N":
        break
    else:
        print("Select the appropriate choice!!")

## Now let's move ahead to find the average frequency of the the orders placed in a particular state
## Create an instance of the state
while True:
    choice = input("Do you want to find the average purchase frequency of a state? Y: Yes, N: No:- ")
    if choice=="Y":
        state_name = input("Enter the state name:- ")
        state = State(state_name)
        average_purchase_frequency = state.average_frequency(transaction_df2)
        print(f'The average purchase frequency of state {state.name} is', average_purchase_frequency)
    elif choice == "N":
        break
    else:
        print("Select the appropriate choice!!")


while True:
    choice = input("Do you want to find the final sales of a particular order? Y: Yes, N: No:- ")
    if choice=="Y":
        order_id = input("Enter the order_id for which you want to find the final sales: ")
        order = OrderDiscount(order_id, transaction_df2)
        print(f"The final sales for the order {order.order_id} placed by the {order.customer} is {order.calculate_total()} \n")
    elif choice == "N":
        break
    else:
        print("Select the appropriate choice!!")