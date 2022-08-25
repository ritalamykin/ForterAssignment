from asyncore import write
from datetime import datetime
from numpy import inner, swapaxes
import pandas as pd
import csv
import jsonlines
import json
import datetime


def convert_csv_to_Forter_schema_for_sellAllToAll(input_path, json_path):
    with open(input_path, newline='') as csvfile, jsonlines.open(json_path, mode='w') as writer:
        reader = csv.DictReader(csvfile)
        for row in reader:
            row_dict = {}
            for k, v in row.items():
                row_dict[k] = (v)
            augment_data_before_dump(row_dict)
            create_nested_entries(row_dict)
            writer.write(row_dict)


def create_nested_entries(row_dict):
    if "firstName" in row_dict:
        field_list_accountOwner = (
            'firstName', 'lastName', 'email', 'accountId')
        create_nested_entries_helper(
            row_dict, field_list_accountOwner, "accountOwner")
    if "name" in row_dict:
        field_list_cartItems = ('name', 'quantity', 'price')
        create_nested_entries_helper(
            row_dict, field_list_cartItems, "cartItems")


def create_nested_entries_helper(row_dict, field_list, new_field):
    inner_dic = {field: row_dict[field] for field in field_list}
    for k in field_list:
        row_dict.pop(k, None)
    if (new_field == "accountOwner"):
        row_dict[new_field] = inner_dic
    elif (new_field == "cartItems"):
        li = []
        li.append(inner_dic)
        row_dict[new_field] = li


def merge_file_into_json(json1, json2):
    with jsonlines.open(json1) as tmp_reader, jsonlines.open(json2) as input_reader, jsonlines.open('final.jsonl', mode='w') as writer:
        tmp_iter = tmp_reader.iter(type=dict)
        input_iter = input_reader.iter(type=dict)
        (tmp, input) = ('', '')
        while tmp != None and input != None:
            tmp = next(tmp_iter, None)
            input = next(input_iter, None)
            if tmp["orderId"] == input["orderId"]:
                break
            else:
                line_to_write = add_default_entries(input)
                writer.write(line_to_write)
                input = next(input_iter, None)
        while tmp != None and input != None:
            if tmp["orderId"] == input["orderId"]:
                writer.write(inner_join_entries(tmp, input, 'orderId'))
                tmp = next(tmp_iter, None)
                input = next(input_iter, None)
            elif tmp["orderId"] < input["orderId"]:
                while (tmp != None and tmp["orderId"] < input["orderId"]):
                    line_to_write = add_default_entries(tmp)
                    writer.write(line_to_write)
                    tmp = next(tmp_iter, None)
            else:
                while (input != None and tmp["orderId"] > input["orderId"]):
                    line_to_write = add_default_entries(input)
                    writer.write(line_to_write)
                    input = next(input_iter, None)
        # Dump last entries
        while (input != None):
            line_to_write = add_default_entries(input)
            writer.write(line_to_write)
            input = next(input_iter, None)
        while (tmp != None):
            line_to_write = add_default_entries(tmp)
            writer.write(line_to_write)
            tmp = next(tmp_iter, None)


def inner_join_entries(tmp, input, idenifier_field):
    input.pop(idenifier_field)
    tmp.update(input)
    return tmp


def add_default_entries(line):
    identifier = determine_identifier(line)
    orders_main = (
        'checkoutTime', 'amountUSD', 'name', 'quantity', 'price')
    accounts = (
        'firstName', 'lastName', 'email', 'accountId', 'amountUSD')
    # Case 1/2 - add orders_main/accounts as empty data
    if (identifier):
        inner_dic = {field: 0 for field in orders_main}
        line['cartItems'] = inner_dic
    else:
        inner_dic = {field: 0 for field in accounts}
        line['accountOwner'] = inner_dic
    return line


def determine_identifier(line):
    if "email" in line:
        return 1
    return 0


def augment_data_before_dump(row_dict):
    swap_field_name(row_dict, "id", "orderId")
    swap_field_name(row_dict, "time", "checkoutTime")
    swap_field_name(row_dict, "item amount (USD)", "price")
    swap_field_name(row_dict, "item name", "name")
    swap_field_name(row_dict, "item quantity", "quantity")
    swap_field_name(row_dict, "orderFullAmount", "amountUSD")
    convert_date_to_epoch(row_dict, "checkoutTime")

# Assumption - using GMT timezone


def convert_date_to_epoch(row_dict, new_field):
    minutes = 0
    hours = 0
    if new_field in row_dict:
        date = row_dict[new_field]
        year = int(date[:4])
        month = int(date[5:7])
        day = int(date[8:10])
        row_dict[new_field] = int((datetime.datetime(
            year, month, day, hours, minutes) - datetime.datetime(1970, 1, 1)).total_seconds())


def swap_field_name(row_dict, old_field, new_field):
    if old_field in row_dict:
        row_dict[new_field] = row_dict.pop(old_field)


def main():
    convert_csv_to_Forter_schema_for_sellAllToAll(
        "/Users/ritalamykin/Desktop/ForterAssignment/sellAllToAll/sellAllToAll_accounts.csv", "temp1.jsonl")
    convert_csv_to_Forter_schema_for_sellAllToAll(
        "/Users/ritalamykin/Desktop/ForterAssignment/sellAllToAll/sellAllToAll_orders_main.csv", 'temp2.jsonl')
    merge_file_into_json("temp1.jsonl", "temp2.jsonl")


if __name__ == "__main__":
    main()
