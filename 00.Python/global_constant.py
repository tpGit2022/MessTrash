#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
from enum import Enum
import os
from pathlib import Path


input_kbf_file_name = ''

output_kbf_file_name = None
full_update = False
step_chain_dict = {
    'unzip_kbf_file': 1,
    'convert_from_kbf_to_sqlite': 1,
    'convert_bill_file': 1,
    'update_db_config': 1,
    'load_need_insert_data': 1,
    'compress_all_file_to_kbf': 1,
    'clear_ssj_all_data': 0,
    'clear_ssj_backup_data': 0
}

bank_pdf_password_list = ["141452", "502135", "154151", "342235", "453135", "454214", "214450", "053051", "034001",
                          "025414", "505043", '144200', '140444', '521401']

alipay_zip_password = 123123123
move_origin_data = True
print_repeat_data_info = False
tp_path = Path(os.path.abspath(__file__))
sdcard_root_path = None
file_store_abs_path = os.path.abspath(__file__)
tag = 'qpython'
ssj_dir_tag = 'ssj'
config_base_dir = os.path.join(tp_path.parent.parent, 'f_input', '00.Config')
ssj_script_input_base_dir = os.path.join(tp_path.parent.parent, 'f_input', ssj_dir_tag)
ssj_script_output_base_dir = os.path.join(tp_path.parent.parent, 'f_output', ssj_dir_tag)
if file_store_abs_path.__contains__(tag):
    end_index = file_store_abs_path.index(tag)
    sdcard_root_path = file_store_abs_path[:end_index]
origin_bill_dir = os.path.join(ssj_script_input_base_dir, 'processed')
remain_alipay_bill_standard_format_data_file_name = 'remain_bill_data_alipay.csv'
remain_weixin_bill_standard_format_data_file_name = 'remain_bill_data_weixin.csv'
remain_china_bank_bill_standard_format_data_file_name = 'remain_bill_data_china_bank.csv'
remain_sw_person_all_data_file_name = 'remain__bill_data_sw_salary_person_all.csv'
remain_pdd_bill_parse_data_file_name = 'remain_bill_data_pdd_order_parse.csv'
all_online_data_to_import_file_name = 'all_online_standard_bill_data_to_import.csv'
new_add_online_data_from_all_data_source = 'new_add_online_bill_data.csv'
har_parse_pdd_order_file = 'pdd_order_parse_data_list.csv'
remain_prefix = 'remain_bill_data_'
tp_prefix = 'tp_'
unique_id = 100
standard_format_data_key = ["交易号", "商家订单号", "交易创建时间", "付款时间", "最近修改时间", "交易对方", "商品名称", "金额", "收/支", "交易状态", "交易方式",
                            "备注", "资金状态", "数据来源", "时间戳"]


def new_unique_id():
    global unique_id
    unique_id = unique_id + 1
    return unique_id


class TradeType(int, Enum):
    Expense = 0
    Income = 1
    Transfer_Step_One = 3
    Transfer_Step_Two = 2


class ConvertType(int, Enum):
    Exchanged = 0
    KbfToSqlite = 1
    SqliteToKbf = 2


class ImportDataSource(str, Enum):
    alipay = 'from_alipay_bill'
    weixin = 'from_weixin_bill'
    china_bank = 'from_china_bank_bill'
    sw = 'from_sw_salary_data'
    pdd_parse = 'from_pdd_parse_order_list'
