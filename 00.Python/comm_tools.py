#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
import csv
import time


def convert_timestamp_to_str(timestamp):
    standard_timestamp = int(int(timestamp) / 1000)
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(standard_timestamp))
    return time_str


def convert_to_timestamp(input_time_str):
    time_array = time.strptime(input_time_str.strip(), "%Y-%m-%d %H:%M:%S")
    time_stamp = int(time.mktime(time_array))
    return time_stamp


def remove_blank_newline_tab(input_str:str):
    return input_str.replace('\n', '').replace('\t', '').strip()


def write_list_values_to_csv_file(file_path:str, write_mode:str, data_list:list):
    with open(file_path, mode=write_mode, encoding='UTF-8', newline='') as f:
        csv_write = csv.writer(f)
        for row in data_list:
            csv_write.writerow(row)


def write_list_dict_values_to_csv_file(file_path:str, write_mode:str, data_list:list):
    with open(file_path, mode=write_mode, encoding='UTF-8', newline='') as f:
        csv_write = csv.writer(f)
        for row in data_list:
            csv_write.writerow(list(row.values()))

