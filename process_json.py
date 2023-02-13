import pandas as pd


final_data = {}

def total_keys(data, keys):
    for key in data.keys():
        if type(data[key]) is dict:
            total_keys(data[key], keys) ## if the value is dict then to traverse inside that dict 
                                        # call the function again else will add the keys directly to the set
        else:
            keys.add(key)
    
    return keys


def fetch_data(data, final_data):
    for key in data.keys():
        if type(data[key]) is dict:
            fetch_data(data[key], final_data)
        else:
            final_data[key].append(data[key])
    
    return final_data

def append_null(fetched_data, max_length):
    for key in fetched_data.keys():
        if len(fetched_data[key]) < max_length:
            fetched_data[key].append(None)

    return fetched_data

if __name__ == "__main__":
    ## Now start extracting the data
    data = [
        {
                "id": 2698,
                "sales_amt": 261.96,
                "qty": 2,
                "discount": 0.0,
                "profit_amt": 41.9136,
                "order": {
                    "order_id": "CA-2014-145317",
                    "ship_mode": "Standard Class",
                    "order_status": "delivered",
                    "order_purchase_date": "2018-07-11 19:46:00",
                    "order_approved_at": "2018-07-13 15:25",
                    "order_delivered_carrier_date": "2018-07-23 15:07",
                    "order_delivered_customer_date": "2018-07-24 14:58",
                    "order_estimated_delivery_date": "2018-07-27",
                    "customer_id": "SM-20320",
                    "VendorID": "VEN03"
                },
                "product": {
                    "product_id": "FUR-BO-10001798",
                    "product_name": "Bush Somerset Collection Bookcase",
                    "colors": "Pink",
                    "category": "Furniture",
                    "sub_category": "Bookcases",
                    "date_added": "2016-04-01",
                    "manufacturer": "null",
                    "sizes": "9",
                    "upc": 640000000000,
                    "weight": "null",
                    "product_photos_qty": 4
                }
            },
            {
                "id": 2698,
                "sales_amt": 261.96,
                "qty": 2,
                "discount": 0.0,
                "profit_amt": 41.9136,
                "order": {
                    "order_id": "CA-2014-145317",
                    "ship_mode": "Standard Class",
                    "order_status": "delivered",
                    "order_purchase_date": "2018-07-11 19:46:00",
                    "order_approved_at": "2018-07-13 15:25",
                    "order_delivered_carrier_date": "2018-07-23 15:07",
                    "order_delivered_customer_date": "2018-07-24 14:58",
                    "order_estimated_delivery_date": "2018-07-27",
                    "customer_id": {
                        "customer_id": "SM-20320",
                        "customer_name": "Sean Miller",
                        "segment": "Home Office",
                        "contact_number": "02342815792",
                        "address": {
                            "zip_code": 77070,
                            "region": "Central",
                            "country": "United States",
                            "city": "houston",
                            "state": "texas"
                        }
                    },
                    "vendor": {
                        "VendorID": "VEN03",
                        "Vendor Name": "Voyage Enterprises"
                    }
                },
                "product": {
                    "product_id": "FUR-BO-10001798",
                    "product_name": "Bush Somerset Collection Bookcase",
                    "colors": "Pink",
                    "category": "Furniture",
                    "sub_category": "Bookcases",
                    "date_added": "2016-04-01",
                    "manufacturer": "null",
                    "sizes": "9",
                    "upc": 640000000000,
                    "weight": "null",
                    "product_photos_qty": 4
                }
            },
            {
                "id": 2698,
                "sales_amt": 261.96,
                "qty": 2,
                "discount": 0.0,
                "profit_amt": 41.9136,
                "order": {
                    "order_id": "CA-2014-145317",
                    "ship_mode": "Standard Class",
                    "order_status": "delivered",
                    "order_purchase_date": "2018-07-11 19:46:00",
                    "order_approved_at": "2018-07-13 15:25",
                    "order_delivered_carrier_date": "2018-07-23 15:07",
                    "order_delivered_customer_date": "2018-07-24 14:58",
                    "order_estimated_delivery_date": "2018-07-27",
                    "customer_id": {
                        "customer_id": "SM-20320",
                        "customer_name": "Sean Miller",
                        "segment": "Home Office",
                        "contact_number": "02342815792",
                        "address": {
                            "zip_code": 77070,
                            "region": "Central",
                            "country": "United States",
                            "city": "houston",
                            "state": "texas"
                        }
                    },
                    "vendor": {
                        "VendorID": "VEN03",
                        "Vendor Name": "Voyage Enterprises"
                    }
                },
                "product_id": "FUR-BO-10001798"
            }
            ]

    # ## find the total_keys
    max_keys = set() ## create an empty set

    ## Now iterate through the whole data and fetch all the keys and store it into a set
    # temp_keys = set() ## create an empty set
    for single_data in data:
        return_keys = total_keys(single_data, max_keys) ## the function will take the empty set and will fill that by keys
        max_keys = max_keys.union(return_keys)

  
    ## Now create an empty dictionary with all the keys
    final_data = {key: [] for key in max_keys}
    for single_data in data:
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
    # transaction_df.to_csv("processed_data.csv")
    print(transaction_df.head())
