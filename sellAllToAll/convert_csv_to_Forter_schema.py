from asyncore import write
from datetime import datetime
from numpy import inner, swapaxes
import pandas as pd
import csv
import jsonlines
import json

# Files' paths
INPUT_ACCOUNTS_PATH = "sellAllToAll/sellAllToAll_accounts.csv"
INPUT_ORDERS_PATH = "sellAllToAll/sellAllToAll_orders_main.csv"
TMP_ACCOUNTS_PATH = "temp1.jsonl"
TMP_ORDERS_PATH = "temp2.jsonl"
OUTPUT_PATH = "final.jsonl"
FRAUDULENT_ACTIVITY_PATH = "fraudulent_activity.jsonl"

# Constant Strings
CART_ITEMS = "cartItems"
QUANTITY = "quantity"
ORDER_ID = "orderId"
ACCOUNT_ID = "accountId"
EMAIL = "email"
FIRST_NAME = "firstName"
LAST_NAME = "lastName"
ACCOUNT_OWNER = "accountOwner"
AMOUNT_USD = "amountUSD"
PRICE = "price"
NAME = "name"
CHECKOUT_TIME = "checkoutTime"
ID = "id"
TIME = "time"
ITEM_AMOUNT_USD = "item amount (USD)"
ITEM_NAME = "item name"
ITEM_QUANTITY = "item quantity"
ORDER_FULL_AMOUNT = "orderFullAmount"


def swap_field_name(dict, old_field_name, new_field_name):
    """
    Args:
        dict: a dictionary
        old_field_name: a string representing the old name of the field
        new_field_name: a string representing the new name of the field

    Replaces all the keys with value old_field_name to new_field_name.
    """
    if old_field_name in dict:
        dict[new_field_name] = dict.pop(old_field_name)


def convert_checkout_time_to_epoch(dict):
    """
    Args:
        dict: a dictionary with the key CHECKOUT_TIME

    Converts the value of the key CHECKOUT_TIME from date format to epoch.
    Note: this function uses the default timezone.
    """
    if CHECKOUT_TIME in dict:
        dict[CHECKOUT_TIME] = int(datetime.strptime(
            dict[CHECKOUT_TIME], '%Y-%m-%d').timestamp())


def convert_row_to_forter_schema(dict):
    """
    Args:
        row_dict: a dictionary

    Returns dict with fields that match Forter's json schema.
    """
    swap_field_name(dict, ID, ORDER_ID)
    swap_field_name(dict, TIME, CHECKOUT_TIME)
    swap_field_name(dict, ITEM_AMOUNT_USD, PRICE)
    swap_field_name(dict, ITEM_NAME, NAME)
    swap_field_name(dict, ITEM_QUANTITY, QUANTITY)
    swap_field_name(dict, ORDER_FULL_AMOUNT, AMOUNT_USD)
    convert_checkout_time_to_epoch(dict)


def create_nested_entries_helper(row_dict, field_list, new_field):
    inner_dic = {field: row_dict[field] for field in field_list}
    for k in field_list:
        row_dict.pop(k, None)
    if (new_field == ACCOUNT_OWNER):
        row_dict[new_field] = inner_dic
    elif (new_field == CART_ITEMS):
        row_dict[new_field] = [inner_dic]


def create_nested_entries(row_dict):
    if FIRST_NAME in row_dict:
        field_list_accountOwner = (FIRST_NAME, LAST_NAME, EMAIL, ACCOUNT_ID)
        create_nested_entries_helper(
            row_dict, field_list_accountOwner, ACCOUNT_OWNER)
    if NAME in row_dict:
        field_list_cartItems = (NAME, QUANTITY, PRICE)
        create_nested_entries_helper(
            row_dict, field_list_cartItems, CART_ITEMS)


def nest_account_information(dict):
    """
    Args:
        dict: a dictionary

    If dict contains ACCOUNT_ID (and therefore contains account fields) then we nest all the account fields under the value ACCOUNT_OWNER.
    """
    if ACCOUNT_ID not in dict:
        return
    account_fields = (FIRST_NAME, LAST_NAME, EMAIL, ACCOUNT_ID)
    inner_dic = {field: dict[field] for field in account_fields}
    # pop the old values
    for key in account_fields:
        dict.pop(key, None)
    dict[ACCOUNT_OWNER] = inner_dic


def parse_accounts(input_path, output_path):
    """
    Args:
        input_path: the input path for the accounts file
        output_path: the input path for the temp accounts file

    Converts the data in input path to a json in a temp file in output_path
    """
    with open(input_path, newline='') as csvfile, jsonlines.open(output_path, mode='w') as writer:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_dict = {}
            for k, v in row.items():
                row_dict[k] = (v)
            convert_row_to_forter_schema(row_dict)
            nest_account_information(row_dict)
            writer.write(row_dict)


def get_cart_items_from_row(dict):
    """
    Args:
        dict: a dictionary representing an order

    Returns the cart items of an order in a dictionary.
    """
    order_fields = (NAME, QUANTITY, PRICE)
    return {field: dict[field] for field in order_fields}


def nest_cart_items(order_dict):
    """
    Args:
        order_dict: a dictionary representing an order

    Nests the cart items in the dictionary.
    """
    cart_items = get_cart_items_from_row(order_dict)
    for key in cart_items:
        order_dict.pop(key, None)
    order_dict[CART_ITEMS] = [cart_items]


def write_time_fraud_line(id, time1, time2):
    """
    Args:
        id: the id of the order
        time1: a time in epoch
        time2: a time in epoch

    Appends a fraud message from the data in the arguments.
    """
    with jsonlines.open(FRAUDULENT_ACTIVITY_PATH, mode='a') as writer:
        writer.write({"message": "possible TIME fraud detected",
                     "orderId": id, "time1": time1, "time2": time2})
        writer.close()


def parse_orders(input_path, output_path):
    """
    Args:
        input_path: the input path for the orders file
        output_path: the input path for the temp orders file

    Converts the data in input path to a json in a temp file in output_path
    """
    with open(input_path, newline='') as csvfile, jsonlines.open(output_path, mode='w') as writer:
        reader_iter = csv.DictReader(csvfile)
        next_row = next(reader_iter, None)
        row_dict = {}
        for k, v in next_row.items():
            row_dict[k] = (v)
        convert_row_to_forter_schema(row_dict)
        while next_row != None:
            nest_cart_items(row_dict)
            curr_id = row_dict[ORDER_ID]
            next_row = next(reader_iter, None)
            if (next_row == None):  # last line in file
                writer.write(row_dict)
                break
            next_row_dict = {}
            for k, v in next_row.items():
                next_row_dict[k] = (v)
            convert_row_to_forter_schema(next_row_dict)
            next_id = next_row_dict[ORDER_ID]
            while next_id == curr_id:
                if next_row_dict[CHECKOUT_TIME] != row_dict[CHECKOUT_TIME]:
                    write_time_fraud_line(
                        next_id, next_row_dict[CHECKOUT_TIME], row_dict[CHECKOUT_TIME])
                row_dict[CART_ITEMS].append(
                    get_cart_items_from_row(next_row_dict))
                next_row = next(reader_iter, None)
                if (next_row == None):
                    break
                for k, v in next_row.items():
                    next_row_dict[k] = (v)
                convert_row_to_forter_schema(next_row_dict)
                next_id = next_row_dict[ORDER_ID]
            writer.write(row_dict)
            row_dict = next_row_dict


def inner_join_entries(tmp, input, idenifier_field):
    input.pop(idenifier_field)
    tmp.update(input)
    return tmp


def merge_file_into_json(accounts, orders):
    """
    Args:
        accounts: a json representing the accounts
        orders: a json representing the accounts

    Merges the jsons from both inputs to the Forter schema.
    """
    with jsonlines.open(accounts) as accounts_reader, jsonlines.open(orders) as orders_reader, jsonlines.open(OUTPUT_PATH, mode='w') as writer:
        accounts_iter = accounts_reader.iter(type=dict)
        orders_iter = orders_reader.iter(type=dict)
        (account_row, order_row) = ('', '')
        account_row = next(accounts_iter, None)
        order_row = next(orders_iter, None)
        # Merge jsons in O(N)
        while account_row != None and order_row != None:
            if account_row[ORDER_ID] == order_row[ORDER_ID]:
                writer.write(inner_join_entries(
                    account_row, order_row, ORDER_ID))
                account_row = next(accounts_iter, None)
                order_row = next(orders_iter, None)
            elif account_row[ORDER_ID] < order_row[ORDER_ID]:
                line_to_write = add_default_entries(account_row)
                writer.write(line_to_write)
                account_row = next(accounts_iter, None)
            else:
                line_to_write = add_default_entries(order_row)
                writer.write(line_to_write)
                order_row = next(orders_iter, None)
        # Dump last entries if we have them
        while (order_row != None):
            line_to_write = add_default_entries(order_row)
            writer.write(line_to_write)
            order_row = next(orders_iter, None)
        while (account_row != None):
            line_to_write = add_default_entries(account_row)
            writer.write(line_to_write)
            account_row = next(accounts_iter, None)


def add_default_entries(line):
    identifier = determine_identifier(line)
    orders_main = (
        CHECKOUT_TIME, NAME, QUANTITY, PRICE)
    accounts = (
        FIRST_NAME, LAST_NAME, EMAIL, ACCOUNT_ID)
    # Case 1/2 - add orders_main/accounts as empty data
    if (identifier):
        inner_dic = {field: 0 for field in orders_main}
        line[CART_ITEMS] = inner_dic
    else:
        line[AMOUNT_USD] = 0
        inner_dic = {field: 0 for field in accounts}
        line[ACCOUNT_OWNER] = inner_dic
    return line


def determine_identifier(line):
    if EMAIL in line:
        return 1
    return 0

# Assumption - using GMT timezone


def identify_fraudulent_totals(json_path):
    with jsonlines.open(json_path) as reader, jsonlines.open(FRAUDULENT_ACTIVITY_PATH, mode='a') as writer:
        json_iter = reader.iter(type=dict)
        entry = next(json_iter, None)
        while entry != None:
            total_amount = int(entry[AMOUNT_USD])
            transaction_price_sum = 0.0
            for item in entry[CART_ITEMS]:
                transaction_price_sum += float(item[PRICE]) * \
                    float(item[QUANTITY])
            if total_amount != transaction_price_sum:
                writer.write({"message": "possible SUM fraud detected",
                              "orderId": entry[ORDER_ID], "totalAmount": total_amount, "totalsFromItems": transaction_price_sum})
            entry = next(json_iter, None)
    reader.close()
    writer.close()


def main():
    parse_accounts(INPUT_ACCOUNTS_PATH, TMP_ACCOUNTS_PATH)
    parse_orders(INPUT_ORDERS_PATH, TMP_ORDERS_PATH)
    merge_file_into_json(TMP_ACCOUNTS_PATH, TMP_ORDERS_PATH)
    identify_fraudulent_totals(OUTPUT_PATH)


if __name__ == "__main__":
    main()
