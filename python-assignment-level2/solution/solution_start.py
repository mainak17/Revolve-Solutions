import argparse
import pandas as pd
import json
import os
import datetime

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

def process_transactions(transactions_location):
    transactions = []
    for root, dirs, files in os.walk(transactions_location):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    for line in f:
                        transaction = json.loads(line)
                        transactions.append(transaction)
    return transactions

def main():
    params = get_params()
    # Read customers.csv 
    customers_df = pd.read_csv(params['customers_location'])
    # print(customers_df)
    # Read products.csv 
    products_df = pd.read_csv(params['products_location'])
    # print(products_df)
    # Read the transactions
    transactions_location = params['transactions_location']
    transactions = process_transactions(transactions_location)
    
    # Merging transactions with customers_df and products_df
    transactions_df = pd.DataFrame(transactions)
    transactions_df = transactions_df.explode('basket')

    #print(transactions_df)

    # Extract product_id from the exploded 'basket' column
    transactions_df['product_id'] = transactions_df['basket'].apply(lambda x: x['product_id'])
    # print(transactions_df.head())
    transactions_df['date_of_purchase'] = pd.to_datetime(transactions_df['date_of_purchase'])
    transactions_df['date'] = transactions_df['date_of_purchase'].dt.date
    #date_of_purchase
    # Merge transactions with customers and products data
    merged_df = pd.merge(transactions_df, customers_df, on='customer_id')
    merged_df = pd.merge(merged_df, products_df, on='product_id')
    #print(merged_df.head())


    # Creating output_df
    
    # Uncomment below to create output_df without date of purchase
    #output_df = merged_df.groupby(['customer_id', 'loyalty_score', 'product_id', 'product_category']).size().reset_index(name='purchase_count') 
    
    # comment below to create output_df without date_of_purchase
    output_df = merged_df.groupby(['customer_id', 'loyalty_score', 'product_id', 'product_category','date']).size().reset_index(name='purchase_count')

    # print(output_df.head())
    # output_df = output_df.sort_values('date_of_purchase') # Uncomment to sort the json by date of purchase
  
    # Convert output dataframe to JSON
    output_json = output_df.to_json(orient='records',indent=2)

    if not os.path.exists(params['output_location']):
        os.makedirs(params['output_location'])
    
    # Write output JSON to a file
    output_file = os.path.join(params['output_location'], 'output.json')
    with open(output_file, 'w') as f:
        f.write(output_json)


if __name__ == "__main__":
    main()
