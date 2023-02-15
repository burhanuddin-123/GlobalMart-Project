import pandas as pd
import numpy as np


class Data_Collection:
    ## Define the constructor
    def __init__(self) -> None:
        pass

    def total_keys(self, data, keys):
        for key in data.keys():
            if type(data[key]) is dict:
                self.total_keys(data[key], keys) ## if the value is dict then to traverse inside that dict 
                                            # call the function again else will add the keys directly to the set
            else:
                keys.add(key)
        
        return keys


    def fetch_data(self, data, final_data):
        for key in data.keys():
            if type(data[key]) is dict:
                self.fetch_data(data[key], final_data)
            else:
                final_data[key].append(data[key])
        
        return final_data

    @classmethod
    def append_null(self, fetched_data, max_length):
        for key in fetched_data.keys():
            if len(fetched_data[key]) < max_length:
                fetched_data[key].append(None)

        return fetched_data

class Product:
    def __init__(self, product_name):
        self.product_name = product_name
        self.sizes = []

    # Create a function that takes the product name and returns all the available sizes for that product
    def product_size(self, df):
        sizes_list = df[df["product_name"]==self.product_name]["sizes"].unique()[0]
        if sizes_list:
            self.sizes = [size.strip() for size in sizes_list.split(',')]
        
        count_of_sizes = len(self.sizes)
        return count_of_sizes

class State:
    def __init__(self, name):
        self.name = name

    ## Function that takes the dataframe and customer_id as input and returns the average purchase frequency of a customer
    def average_frequency(self, df):
        try:
            ## Filter the records of that particular state
            filter_df = df[df["state"]==self.name]
            print(filter_df)

            ## Now check if there is any record of the given state or not
            if len(filter_df)==0:
                raise Exception(f"There is no such state called {self.name} in the data")

            
            state_df = filter_df[["order_purchase_date"]]
            state_df = state_df.drop_duplicates(keep='last')
            state_df["order_purchase_date"] = pd.to_datetime(state_df["order_purchase_date"])
            print(state_df)
            
            # print(filter_df2)
            
            ## Now sort the order purchase date
            state_df.sort_values("order_purchase_date", ascending=True, inplace=True)
            state_df["next_purchase_date"] = state_df["order_purchase_date"].shift(-1)
            state_df["purchase_frequency"] = np.round((state_df["next_purchase_date"] - state_df["order_purchase_date"]) / np.timedelta64(1, 'D'), 2)
            print(state_df)
            avg_purchase_frequency = np.round(state_df["purchase_frequency"].mean(), 2)
            return avg_purchase_frequency
        except Exception as err:
            print(err)



class Order:
    def __init__(self, order_id, df):
        self.order_id = order_id
        # self.customer = list(df[df["order_id"]==self.order_id]["customer_name"].values)[0]
        self.transactions = list(df[df["order_id"]==self.order_id]["sales_amt"].values)

    def calculate_total(self):
        total = 0
        for transaction_amt in self.transactions:
            total += transaction_amt
        return total

class OrderDiscount(Order):
    orders_list = []

    def __init__(self, order_id, df):
        super().__init__(order_id, df)
        self.discount = list(df[df["order_id"]==self.order_id]["discount"].values)
        OrderDiscount.orders_list.append(self.order_id)

    def calculate_total(self):
        total = 0
        for index in range(len(self.transactions)):
            transaction_amt = self.transactions[index]*(1-self.discount[index])
            total += transaction_amt
        return total

    @classmethod
    def get_orders(cls):
        return OrderDiscount.orders_list


# class OrderService:
#     def __init__(self, price):
#         self.price = price

#     def place_order(self, order):
#         total = order.calculate_total()
#         if total is None:
#             return None
#         elif self.price>=total:
#             return True
        
#         return False



if __name__ == "__main__":

    from process_json import Data_Collection, Product, State, OrderDiscount

    # ## 
    order_df = pd.DataFrame([{"id": 1, "order_id": 1, "sales_amt": 261.96, "discount": 0.2}, {"id": 2, "order_id": 2, "sales_amt": 865, "discount": 0.3}, 
                            {"id": 3, "order_id": 2, "sales_amt": 560, "discount": 0.4}, {"id": 4, "order_id": 3, "sales_amt": 1765, "discount": 0.5},
                            {"id": 5, "order_id": 4, "sales_amt": 5000, "discount": 0.6}, {"id": 6, "order_id": 4, "sales_amt": 1500, "discount": 0.8}])

    order1 = OrderDiscount(1, order_df)
    # print(order.order_id)
    # print(order.transactions)
    # print(order.discount)
    # print(order.calculate_total())

    order2 = OrderDiscount(2, order_df)
    order3 = OrderDiscount(3, order_df)
    order4 = OrderDiscount(4, order_df)

    ## Now let's call the class method
    print(OrderDiscount.get_orders())

    # print(order.calculate_total())
    # if not order.transactions:
    #     print("There is no such order !!!")
    #     del order
    #     exit()

    # ## Now let's check whether we have that much price available to place the order or not
    # order_service = OrderService(100)
    # if order_service.place_order(order):
    #     print("Yay! The order is placed")
    # else:
    #     print("The balance is low!!")
    
    exit()
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
                    "order_purchase_date": "2018-08-11 19:46:00",
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
                    "order_purchase_date": "2018-06-11 19:46:00",
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

    ## Create an object of API_Collection
    api = Data_Collection()

    # ## find the total_keys
    max_keys = set() ## create an empty set

    ## Now iterate through the whole data and fetch all the keys and store it into a set
    # temp_keys = set() ## create an empty set
    for single_data in data:
        return_keys = api.total_keys(single_data, max_keys) ## the function will take the empty set and will fill that by keys
        max_keys = max_keys.union(return_keys)

  
    ## Now create an empty dictionary with all the keys
    final_data = {key: [] for key in max_keys}
    for single_data in data:
        final_data = api.fetch_data(single_data, final_data)

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
            final_data = Data_Collection.append_null(final_data, max_len) ## This is the class method

    
    transaction_df = pd.DataFrame(final_data)

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
    state = State('texas')
    print(state.name)

    average_purchase_frequency = state.average_frequency(transaction_df2)
    print(average_purchase_frequency)