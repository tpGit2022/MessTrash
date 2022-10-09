#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
import os.path
import sqlite3
import zipfile
import csv
import time
from ssj_decide_category import decide_category
from ssj_decide_category import new_unique_id
import ssj_decide_category
from ssj_decide_category import TradeType
from enum import Enum
import shutil
from pathlib import Path


class ConvertType(int, Enum):
    Exchanged = 0
    KbfToSqlite = 1
    SqliteToKbf = 2


target_file = ''
# ot_file_name = '流水核验_20220929163911_u_nquxyn1_60767968717.kbf'
ot_file_name = None
step_chain_dict = {
    'unzip_kbf_file': 1,
    'convert_from_kbf_to_sqlite': 1,
    'write_data_to_sqlite': 1,
    'compress_all_file_to_kbf': 1,
    'clear_ssj_all_data': 0,
    'clear_ssj_backup_data': 0
}
tp_file = os.path.abspath(__file__)
tp_path = Path(tp_file)
data_transaction_model_keys = ["transactionPOID", "createdTime", "modifiedTime", "tradeTime", "memo", "type",
                               "creatorTradingEntityPOID", "modifierTradingEntityPOID", "buyerAccountPOID",
                               "buyerCategoryPOID",
                               "buyerMoney", "sellerAccountPOID", "sellerCategoryPOID", "sellerMoney", "lastUpdateTime",
                               "photoName", "photoNeedUpload", "relation", "relationUnitPOID", "ffrom", "clientID",
                               "FSourceKey",
                               "photos", "transaction_number", "merchant_order_number", "import_data_source"]
data_account_model_keys = ["accountPOID", "name", "tradingEntityPOID", "accountGroupPOID"]
data_category_model_keys = ["categoryPOID", "name", "parentCategoryPOID", "path", "depth", "userTradingEntityPOID",
                            "type", "clientID"]
standard_format_data_key = ["交易号", "商家订单号", "交易创建时间", "付款时间", "最近修改时间", "交易对方", "商品名称", "金额", "收/支", "交易状态", "交易方式", "服务费", "备注", "资金状态", "数据来源"]


def unzip_kbf(input_files=None):
    if input_files is None:
        input_files = os.path.join(tp_path.parent.parent, 'f_input/')
    if not os.path.exists(input_files):
        print(f'{input_files}不存在')
    files = os.listdir(input_files)
    for f in files:
        if f.endswith('.kbf'):
            global target_file
            target_file = f
            zf = zipfile.ZipFile(os.path.join(input_files, f))
            zf.extractall(path=input_files)
            break


def get_config_in_database(sqlite3_path=None):
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')

    print(f"数据库路径:{sqlite3_path}")

    conn = sqlite3.connect(sqlite3_path)
    cur = conn.cursor()
    query_account_info_sql = """select accountPOID,name,tradingEntityPOID,accountGroupPOID from t_account;"""
    ret = cur.execute(query_account_info_sql)
    account_info_dict = {}
    for row in ret:
        account_info_dict[row[1]] = row[0]

    query_category_info_sql = """select categoryPOID,name,parentCategoryPOID,path,depth,userTradingEntityPOID,type,clientID from t_category;"""
    ret = cur.execute(query_category_info_sql)
    category_info_dict = {}
    for row in ret:
        category_info_dict[row[1]] = row[0]

    print(account_info_dict)
    print()
    print(category_info_dict)
    query_ret = """select transactionPOID from t_transaction ORDER By t_transaction.transactionPOID DESC limit 1 ;"""
    ret = cur.execute(query_ret)
    t = ret.fetchone()
    if t is not None:
        last_transaction_poid = t[0]
        if last_transaction_poid < 0:
            last_transaction_poid = -last_transaction_poid
        ssj_decide_category.unique_id = last_transaction_poid + 1
        print(f'修正初始unique_id为{ssj_decide_category.unique_id}')
    cur.close()
    conn.close()
    return account_info_dict, category_info_dict


def convert_alipay_bill_to_standard_data_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_data_list = []
    for f in files:
        if f.startswith("alipay_record") and f.endswith(".csv"):
            with open(f'{input_dir}{f}', 'r', encoding='GBK') as rf:
                reader = csv.reader(rf)
                data_header = []
                data_read_flag = False
                for row in reader:
                    if row[0].strip().__eq__('交易号'):
                        data_read_flag = True
                        data_header = row
                        continue
                    if data_read_flag:
                        if len(row) < 5:
                            continue
                        standard_data = dict.fromkeys(standard_format_data_key)
                        column_index = 0
                        while column_index < max(len(data_header), len(standard_format_data_key)):
                            standard_data[data_header[column_index].strip()] = row[column_index].replace('\t', '').strip()
                            column_index = column_index + 1
                        standard_data['金额'] = standard_data['金额（元）']
                        standard_data['服务费'] = standard_data['服务费（元）']
                        standard_data.pop('金额（元）')
                        standard_data.pop('服务费（元）')
                        standard_data.pop('交易来源地')
                        standard_data.pop('类型')
                        standard_data.pop('成功退款（元）')
                        standard_data['交易方式'] = '支付宝'
                        if standard_data['资金状态'] is None:
                            standard_data['资金状态'] = ' '
                        if standard_data['备注'] is None:
                            standard_data['备注'] = ' '
                        standard_data['备注'] = f'{standard_data["备注"]}\n数据来源:支付宝'
                        standard_data['数据来源'] = 'from_alipay_bill'
                        standard_data_list.append(standard_data)
    print(f'支付宝待插入数据{len(standard_data_list)}条')
    return standard_data_list


def convert_weixin_bill_to_standard_data_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    standard_weixin_bill_csv = os.path.join(tp_path.parent.parent, 'f_input/standard_bill_from_weixin.csv')
    files = os.listdir(input_dir)
    standard_data_list = []
    for f in files:
        if f.startswith("微信支付账单") and f.endswith(".csv"):
            with open(f'{input_dir}{f}', 'r', encoding="UTF-8") as rf:
                reader = csv.reader(rf)
                data_header = []
                data_read_flag = False
                for row in reader:
                    if row[0].strip().__eq__('交易时间'):
                        data_read_flag = True
                        data_header = row
                        continue
                    if data_read_flag:
                        if len(row) < 5:
                            continue
                        standard_data = dict.fromkeys(standard_format_data_key)
                        column_index = 0
                        while column_index < min(len(data_header), len(standard_format_data_key)):
                            standard_data[data_header[column_index].strip()] = row[column_index].replace('\t', '').strip()
                            column_index = column_index + 1
                        standard_data['金额'] = standard_data['金额(元)'].replace('¥', '')
                        standard_data['交易创建时间'] = standard_data['交易时间']
                        standard_data['付款时间'] = standard_data['交易时间']
                        standard_data['最近修改时间'] = standard_data['交易时间']
                        standard_data['商品名称'] = standard_data['商品']
                        standard_data['交易号'] = standard_data['交易单号']
                        standard_data['商家订单号'] = standard_data['商户单号']
                        standard_data['交易状态'] = '交易成功'
                        standard_data['服务费'] = '0'

                        pay_channel = standard_data['支付方式']
                        if pay_channel.__contains__('银行'):
                            standard_data['交易方式'] = '银行卡'
                        else:
                            standard_data['交易方式'] = '微信钱包'

                        if standard_data['资金状态'] is None:
                            standard_data['资金状态'] = ' '
                        if standard_data['备注'] is None:
                            standard_data['备注'] = ' '
                        standard_data['备注'] = standard_data['备注'].replace('/', '')
                        standard_data['备注'] = f'{standard_data["备注"]}{standard_data["当前状态"]}数据来源:微信短账单'
                        standard_data['数据来源'] = 'from_weixin_bill'
                        standard_data.pop('交易时间')
                        standard_data.pop('商品')
                        standard_data.pop('金额(元)')
                        standard_data.pop('交易类型')
                        standard_data.pop('交易单号')
                        standard_data.pop('商户单号')
                        standard_data.pop('支付方式')
                        standard_data.pop('当前状态')
                        standard_data_list.append(standard_data)

    if not os.path.exists(standard_weixin_bill_csv):
        csv_file = open(standard_weixin_bill_csv, mode='w+', encoding='UTF-8', newline='')
        csv_write = csv.writer(csv_file)
        csv_write.writerow(standard_format_data_key)
    else:
        csv_file = open(standard_weixin_bill_csv, mode='a+', encoding='UTF-8', newline='')
        csv_write = csv.writer(csv_file)
    for data in standard_data_list:
        w_data = list(data.values())
        w_data[0] = w_data[0] + '\t'
        w_data[1] = w_data[1] + '\t'
        csv_write.writerow(w_data)
    csv_file.close()
    print(f'微信待插入数据{len(standard_data_list)}条')
    # print(standard_data_list)
    return standard_data_list


def convert_weixin_long_bill_to_standard_data_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_data_list = []
    for f in files:
        if f.startswith("standard_bill_from_weixin") and f.endswith(".csv"):
            with open(f'{input_dir}{f}', 'r', encoding='UTF-8') as rf:
                reader = csv.reader(rf)
                data_header = []
                is_first = True
                for row in reader:
                    if is_first:
                        data_header = row
                        is_first = False
                        continue
                    standard_data = dict.fromkeys(standard_format_data_key)
                    column_index = 0
                    while column_index < len(data_header):
                        standard_data[data_header[column_index]] = row[column_index].replace('\t', '')
                        column_index = column_index + 1
                    if standard_data['备注'] is None:
                        standard_data['备注'] = ' '
                    standard_data['备注'] = f'{standard_data["备注"]}\n数据来源:微信支付'
                    if standard_data['交易方式'] is not None:
                        if standard_data['交易方式'].strip().__contains__('零钱'):
                            standard_data['交易方式'] = '微信钱包'
                        elif standard_data['交易方式'].strip().__contains__('银行'):
                            standard_data['交易方式'] = '银行卡'
                        elif standard_data['交易方式'].strip().__eq__('/'):
                            standard_data['交易方式'] = '微信钱包'
                    else:
                        standard_data['交易方式'] = '微信钱包'
                    standard_data['数据来源'] = 'from_weixin_bill'
                    standard_data_list.append(standard_data)
    print(f'微信待插入数据{len(standard_data_list)}条')
    return standard_data_list
    pass


def standard_bill_file_analysis_tool(account_info_dict, category_info_dict, standard_format_data_list):
    """
    using alipay bill csv file as standard to analysis, the bank bill file or weixin bill file need to convert standard before
    analysis
    :param standard_format_data_list: the standard data format which convert from alipay, weixin, bank bill
    :param account_info_dict: the ssj account info
    :param category_info_dict: the ssj category info
    :return:
    """
    if account_info_dict is None or category_info_dict is None or standard_format_data_list is None:
        print(f"account_info_dict或者category_info_dict为空")
        return
    ssj_data_list = []
    visited_data_dict = {}
    repeat_record_count = 0
    for standard_data in standard_format_data_list:
        if not visited_data_dict.keys().__contains__(standard_data['数据来源']):
            visited_data_dict[standard_data['数据来源']] = set()
        if visited_data_dict[standard_data['数据来源']].__contains__(standard_data['交易号']):
            # print(f'重复交易号-->{standard_data}')
            repeat_record_count = repeat_record_count + 1
            continue

        visited_data_dict[standard_data['数据来源']].add(standard_data['交易号'])
        time_format = '%Y-%m-%d %H:%M:%S'
        ssj_data_model = dict.fromkeys(data_transaction_model_keys)
        ssj_data_model['transactionPOID'] = new_unique_id()
        if standard_data['交易创建时间'] is None or len(standard_data['交易创建时间']) == 0 or len(standard_data['交易创建时间'].strip()) == 0:
            ssj_data_model['createdTime'] = standard_data['交易创建时间']
        else:
            time_array = time.strptime(standard_data['交易创建时间'].strip(), time_format)
            timestamp = int(time.mktime(time_array)) * 1000
            ssj_data_model['createdTime'] = timestamp
        
        if standard_data['最近修改时间'] is None or len(standard_data['最近修改时间']) == 0 or len(standard_data['最近修改时间'].strip()) == 0:
            ssj_data_model['modifiedTime'] = standard_data['最近修改时间']
        else:
            time_array = time.strptime(standard_data['最近修改时间'].strip(), time_format)
            timestamp = int(time.mktime(time_array)) * 1000
            ssj_data_model['modifiedTime'] = timestamp

        if standard_data['付款时间'] is None or len(standard_data['付款时间']) == 0 or len(standard_data['付款时间'].strip()) == 0:
            ssj_data_model['tradeTime'] = ssj_data_model['createdTime']
        else:
            time_array = time.strptime(standard_data['付款时间'].strip(), time_format)
            timestamp = int(time.mktime(time_array)) * 1000
            ssj_data_model['tradeTime'] = timestamp

        ssj_data_model['memo'] = f"交易对方:{standard_data['交易对方']}\n商品名称:{standard_data['商品名称']}\n收/支:{standard_data['收/支']}\n交易状态:{standard_data['交易状态']}\n" \
                                 f"手续费:{standard_data['服务费']}\n交易方式:{standard_data['交易方式']}\n资金状态:{standard_data['资金状态']}\n交易时间:{standard_data['交易创建时间']}\n交易号:{standard_data['交易号']}\n商家订单号:{standard_data['商家订单号']}\n备注:{standard_data['备注']}"
        ssj_data_model['type'] = 0
        ssj_data_model['creatorTradingEntityPOID'] = -3
        ssj_data_model['modifierTradingEntityPOID'] = -3
        ssj_data_model['buyerAccountPOID'] = 0
        ssj_data_model['buyerCategoryPOID'] = 0
        ssj_data_model['buyerMoney'] = standard_data['金额']
        ssj_data_model['sellerAccountPOID'] = 0
        ssj_data_model['sellerCategoryPOID'] = 0
        ssj_data_model['sellerMoney'] = standard_data['金额']
        ssj_data_model['lastUpdateTime'] = ssj_data_model['createdTime']
        ssj_data_model['photoName'] = ''
        ssj_data_model['photoNeedUpload'] = 0
        ssj_data_model['relation'] = ''
        ssj_data_model['relationUnitPOID'] = 0
        ssj_data_model['ffrom'] = 'Android-MyMoney For Huawei-version-10.3.8.5-Insert'
        ssj_data_model['clientID'] = ssj_data_model['transactionPOID']
        ssj_data_model['FSourceKey'] = ''
        ssj_data_model['photos'] = ''
        ssj_data_model['transaction_number'] = standard_data['交易号'].strip()
        ssj_data_model['merchant_order_number'] = standard_data['商家订单号'].strip()
        ssj_data_model['import_data_source'] = standard_data['数据来源'].strip()
        # do specific pick thing
        if standard_data['交易状态'].__contains__('失败') or standard_data['交易状态'].__contains__('关闭'):
            continue

        if standard_data['收/支'].__eq__('收入'):
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data, ssj_data_list)
            ssj_data_model['type'] = TradeType.Income.value
            ssj_data_model['buyerAccountPOID'] = 0
            ssj_data_model['sellerAccountPOID'] = account_info_dict[f"{standard_data['交易方式']}"]
            ssj_data_model['sellerCategoryPOID'] = 0
            if ssj_data_model['buyerCategoryPOID'] is None or (isinstance(ssj_data_model['buyerCategoryPOID'], int) and ssj_data_model['buyerCategoryPOID'] == 0):
                ssj_data_model['buyerCategoryPOID'] = category_info_dict['未分类收入']
                ssj_data_model['memo'] = f"{ssj_data_model['memo']}\n纳入未分类收入"
        elif standard_data['收/支'].__eq__('支出'):
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data, ssj_data_list)
            ssj_data_model['type'] = TradeType.Expense.value
            ssj_data_model['buyerAccountPOID'] = account_info_dict[f"{standard_data['交易方式']}"]
            ssj_data_model['buyerCategoryPOID'] = 0
            ssj_data_model['sellerAccountPOID'] = 0
            if ssj_data_model['sellerCategoryPOID'] is None or (isinstance(ssj_data_model['sellerCategoryPOID'], int) and ssj_data_model['sellerCategoryPOID'] == 0):
                ssj_data_model['sellerCategoryPOID'] = category_info_dict['其他支出']
                ssj_data_model['memo'] = f"{ssj_data_model['memo']}\n纳入未分类支出"
        else:
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data, ssj_data_list)
        if not need_jump:
            ssj_data_list.append(ssj_data_model)
    if repeat_record_count > 0:
        print(f'重复记录{repeat_record_count}条')
    return ssj_data_list
    pass


def ssj_kbf_sqlite_convert(input_file=None, output_file=None, convert=ConvertType.Exchanged):
    """
    convert ssj data, after kbf unzip to sqlite,convert it to normal sqlite database file
    :param convert: 0 means convert it auto, 1 kbf format to sqlite, 2 sqlite to kbf format
    :param input_file: the mymoney.sqlite file path
    :param output_file: the convert mymoney.sqlite file path
    :return:
    """
    if input_file is None:
        input_file = os.path.join(tp_path.parent.parent, 'f_input/mymoney.sqlite')
    if output_file is None:
        output_file = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')
    sqlite_header = (0x53, 0x51, 0x4C, 0x69,
                     0x74, 0x65, 0x20, 0x66,
                     0x6F, 0x72, 0x6D, 0x61,
                     0x74, 0x20, 0x33, 0x0)
    kbf_header = (0x0, 0x0, 0x0, 0x0,
                  0x0, 0x0, 0x0, 0x0,
                  0x0, 0x0, 0x0, 0x0,
                  0x0, 0x46, 0xFF, 0x0)
    read_file_header(input_file)
    if os.path.exists(output_file):
        os.remove(output_file)
    with open(input_file, mode='rb') as f:
        with open(output_file, mode='wb') as fw:
            data_buffer = f.read()
            if data_buffer[0] == 0x53:
                kbf2sqlite = False
                print("当前为SQLite文件格式")
            if data_buffer[0] == 0x00:
                kbf2sqlite = True
                print("当前为KBF文件格式")
            write_buffer = bytearray(data_buffer)
            index = 0
            while index < len(kbf_header) and index < len(sqlite_header):
                if convert == ConvertType.Exchanged:
                    if kbf2sqlite is True:
                        write_buffer[index] = sqlite_header[index]
                    else:
                        write_buffer[index] = kbf_header[index]
                elif convert == ConvertType.SqliteToKbf and not kbf2sqlite:
                    write_buffer[index] = kbf_header[index]
                elif convert == ConvertType.KbfToSqlite and kbf2sqlite:
                    write_buffer[index] = sqlite_header[index]

                index = index + 1
            fw.write(write_buffer)
            pass
    read_file_header(output_file)
    pass


def read_file_header(file_path):
    if not os.path.exists(file_path) or file_path is None:
        return
    print(f"{file_path} fileHeader:")
    with open(file_path, mode='rb') as f:
        data_bytes = bytearray(f.read(16))
        for i in data_bytes:
            print("{:04X}".format(i), end=' ')
    print()


def bytes_convert(data_bytes):
    if data_bytes is None or len(data_bytes) <= 0:
        return
    index = 0
    while index < len(data_bytes):
        data_bytes[index] = data_bytes[index] ^ (127 - (index & 63))
        index = index + 1
    pass


def covert_ssj_data_backup_info_file():
    input_file = os.path.join(tp_path.parent.parent, 'f_input/backup_info')
    output_file = os.path.join(tp_path.parent.parent, 'f_output/backup_info')
    with open(input_file, mode='rb+') as f:
        with open(output_file, mode='wb+') as fw:
            origin_data = bytearray(f.read())
            bytes_convert(origin_data)
            fw.write(origin_data)
    pass


def adb_helper_clear_ssj_data(operation):
    os.system(r"adb devices -l")
    os.system(r"adb devices -l")
    if operation == 1:
        # pull file
        os.system(r"adb shell ls -al /sdcard/.mymoney/backup/")
        os.system(r"adb pull /sdcard/.mymoney/backup/ C:/Users/Walkers/Desktop/ApkData/com.mymoney/")
        return
        pass
    if operation == 2:
        # push file
        os.system(r"adb shell rm -rf /sdcard/.mymoney/backup/")
        os.system(r"adb shell mkdir /sdcard/.mymoney/backup/")
        # os.system(r"adb push C:/Users/Walkers/Desktop/ApkData/默认账本_20220902091252.kbf /sdcard/.mymoney/backup/")
        pass
    if operation == 3:
        os.system(r"adb shell su -c ls -al /data/data/com.mymoney/databases/")
        os.system(r"adb shell su -c pm clear com.mymoney")
        os.system(r"adb shell su -c ls -al /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c rm -rf /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c mkdir /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c ls -al /sdcard/.mymoney/backup/")


def update_database_table_struct(sqlite3_path=None):
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')
    print(f"数据库路径:{sqlite3_path}")
    conn = sqlite3.connect(sqlite3_path)
    cur = conn.cursor()

    # add field
    field_need_insert_tuple = ('transaction_number', 'merchant_order_number', 'import_data_source')
    for field_data in field_need_insert_tuple:
        query_ret = f"select * from sqlite_master where name='t_transaction' and sql like '%{field_data}%';"
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            print(f"更新表 t_transaction 结构，新增字段 --> {field_data}")
            alter_table_add_trade_number_sql = f"alter table 't_transaction' add {field_data} TEXT;"
            cur.execute(alter_table_add_trade_number_sql)

    # add account
    query_ret = """select * from t_id_seed WHERE t_id_seed.tableName == 't_account'"""
    ret = cur.execute(query_ret)
    id_seed_tuple = ret.fetchone()
    if id_seed_tuple[0] != 't_account':
        print('数据库未找到t_account!!!')
        return
    account_id_seed = id_seed_tuple[1]
    insert_account_sql = """INSERT INTO "t_account" ("accountPOID", "name", "tradingEntityPOID", "lastUpdateTime", "usedCount", "accountGroupPOID", "balance", "currencyType", "memo", "amountOfLiability", "amountOfCredit", "ordered", "hidden", "parent", "clientID", "uuid", "iconName", "countedOutAssets") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    insert_account_data = []

    account_need_insert_tuple = ('花呗', '借呗', '招商金融', '度小满金融', '京东白条', '京东金条', '苏宁金融', '微粒贷', '微信钱包')
    for account_name in account_need_insert_tuple:
        query_ret = """select * from t_account WHERE name == '花呗'"""
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            print(f"表 t_account 数据，需新增账户 --> {account_name}")
            account_tuple_tp = (-account_id_seed, account_name, '-3', '1663640017823', '0', '9', '0', 'CNY', '', '0', '0', '0', '0', '0', -account_id_seed, '', 'zhang_hu_xuni', '0')
            account_id_seed = account_id_seed + 1
            insert_account_data.append(account_tuple_tp)
    cur.executemany(insert_account_sql, insert_account_data)
    conn.commit()
    query_ret = """select accountPOID,name from t_account;"""
    ret = cur.execute(query_ret)
    for row in ret:
        print(row)

    update_id_seed_sql = f"UPDATE t_id_seed set idSeed = {account_id_seed} WHERE tableName == 't_account'"
    cur.execute(update_id_seed_sql)
    conn.commit()
    query_ret = """select * from t_id_seed WHERE t_id_seed.tableName == 't_account'"""
    ret = cur.execute(query_ret)
    id_seed_tuple = ret.fetchone()
    print(id_seed_tuple)
    # 新增支出分类 休闲娱乐-福彩，休闲娱乐-体彩，食品酒水-食材，金融保险-保险费
    query_ret = """SELECT categoryPOID,name,parentCategoryPOID,path from t_category where depth == '1';"""
    ret = cur.execute(query_ret)
    category_first_level_dict = {}
    for row in ret:
        category_first_level_dict[row[1]] = row

    query_ret = """select * from t_id_seed WHERE t_id_seed.tableName == 't_category'"""
    ret = cur.execute(query_ret)
    id_seed_tuple = ret.fetchone()
    if id_seed_tuple[0] != 't_category':
        print('数据库未找到t_category!!!')
        return
    category_id_seed = id_seed_tuple[1] + 100
    insert_category_sql = """INSERT INTO "t_category" ("categoryPOID", "name", "parentCategoryPOID", "path", "depth", "lastUpdateTime", "userTradingEntityPOID", "_tempIconName", "usedCount", "type", "ordered", "clientID", "iconName", "hidden") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    insert_category_data = []

    # 0 支出 1 收入
    category_need_insert_data_dict = {'福彩': ('0', '休闲娱乐'), '体彩': ('0', '休闲娱乐'), '食材': ('0', '食品酒水'), '保险费': ('0', '金融保险'),
                                      '未分类支出': ('0', '其他杂项'), '贷款借入': ('1', '其他收入'), '未分类收入': ('1', '其他收入')}
    for item in category_need_insert_data_dict.items():
        key = item[0]
        value = item[1]
        query_ret = f"select * from t_category where  t_category.name == '{key}'"
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            print(f"表 t_category 数据，需新增类型 --> {key}")
            category_key = value[1]
            category_tuple_tp = (-category_id_seed, key, category_first_level_dict[f'{category_key}'][0], f"{category_first_level_dict[f'{category_key}'][3]}{-category_id_seed}/", '2', '1663639743742', '-3', 'defaultIcon', '0', value[0], '0', -category_id_seed, 'icon_qtzx', '0')
            category_id_seed = category_id_seed + 1
            insert_category_data.append(category_tuple_tp)

    cur.executemany(insert_category_sql, insert_category_data)
    conn.commit()
    update_id_seed_sql = f"UPDATE t_id_seed set idSeed = {category_id_seed} WHERE tableName == 't_category'"
    cur.execute(update_id_seed_sql)
    conn.commit()
    cur.close()
    conn.close()
    pass


def write_ssj_data_to_database(sqlite3_path=None, ssj_data_set=None):
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')

    print(f"数据库路径:{sqlite3_path}")
    conn = sqlite3.connect(sqlite3_path)
    cur = conn.cursor()
    ret = cur.execute('select count(*) FROM t_transaction;')
    data_count = ret.fetchone()
    print(f"表已有数据{data_count[0]}条")
    query_ret = """select transaction_number from t_transaction;"""
    ret = cur.execute(query_ret)
    inserted_set = set(ret)
    if ssj_data_set is None:
        print("无待插入数据")
        return
    insert_data_sql = """INSERT INTO 't_transaction' ('transactionPOID','createdTime','modifiedTime','tradeTime','memo','type','creatorTradingEntityPOID','modifierTradingEntityPOID','buyerAccountPOID','buyerCategoryPOID','buyerMoney','sellerAccountPOID','sellerCategoryPOID','sellerMoney','lastUpdateTime','photoName','photoNeedUpload','relation','relationUnitPOID','ffrom','clientID','FSourceKey','photos', 'transaction_number', 'merchant_order_number', 'import_data_source') VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    insert_data_list = []
    for data in ssj_data_set:
        # doing this because inserted_set element is tuple, so we need convert it
        if not inserted_set.__contains__((data['transaction_number'],)):
            insert_data_list.append(tuple(data.values()))
    if len(insert_data_list) == 0:
        print('无待更新数据...')
        return
    cur.executemany(insert_data_sql, insert_data_list)
    conn.commit()
    ret = cur.execute('select count(*) FROM t_transaction;')
    after_insert_data_count = ret.fetchone()
    print(f"表新增数据{int(after_insert_data_count[0]) - int(data_count[0])}条,共计{int(after_insert_data_count[0])}条")
    query_ret = """select transactionPOID from t_transaction ORDER By t_transaction.transactionPOID DESC limit 1;"""
    ret = cur.execute(query_ret)
    last_transaction_poid = ret.fetchone()[0]
    print(f"last_transaction_poid --> {last_transaction_poid}")
    if last_transaction_poid < 0:
        last_transaction_poid = -last_transaction_poid
    update_id_seed_sql = f"UPDATE t_id_seed set idSeed = {int(last_transaction_poid) + 1} WHERE tableName == 't_transaction'"
    cur.execute(update_id_seed_sql)
    conn.commit()
    query_ret = """select * from t_id_seed WHERE t_id_seed.tableName == 't_transaction'"""
    ret = cur.execute(query_ret)
    id_seed_tuple = ret.fetchone()
    print(id_seed_tuple)
    cur.close()
    conn.close()
    pass


def get_phone_ssj_data():
    os.system(r"echo %TEMP%")
    os.system(r"rd /S /Q %TEMP%\\ssj_data\\")
    os.system(r"mkdir %TEMP%\\ssj_data\\")
    os.system(r"adb pull /sdcard/.mymoney/backup/ %TEMP%/ssj_data/")
    # unzip kbf
    base_dir = os.environ["TEMP"]
    target_dir = f"{base_dir}{os.path.sep}ssj_data{os.path.sep}backup{os.path.sep}"
    print(target_dir)
    list_file = os.listdir(target_dir)
    for f in list_file:
        if f.endswith('.kbf'):
            unzip_dir = f"{target_dir}{f[:-4]}"
            if os.path.exists(unzip_dir):
                os.remove(unzip_dir)
            os.mkdir(unzip_dir)
            print(f"解压目录:{unzip_dir}")
            with zipfile.ZipFile(f"{target_dir}{f}", 'r') as zipf:
                zipf.extractall(unzip_dir)
            input_file = f"{unzip_dir}{os.path.sep}mymoney.sqlite"
            if os.path.exists(input_file):
                output_file = f"{input_file}_k_s"
                ssj_kbf_sqlite_convert(input_file, output_file)
                write_ssj_data_to_database(output_file)
                os.system(f"start {unzip_dir}")
    # convert to normal sqlite database file
    # del_dir = subprocess.Popen(["cmd.exe", "/c", "dir", "C:\\"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # out,error = del_dir.communicate()
    # print(out.decode('GBK'))

    pass


def compress_file_to_kbf(input_dir=None, output_file=None):
    """

    :param input_dir:
    :param output_file:
    :return:
    """
    if input_dir is None:
        input_dir = os.path.join(tp_path.parent.parent, 'f_output/')
    if output_file is None:
        # output_file = '../f_output/流水核验_20220925173711_u_nquxyn1_60767968717.kbf'
        output_file = os.path.join(tp_path.parent.parent, 'f_output/默认账本_20220929133647.kbf')
    if os.path.exists(output_file):
        os.remove(output_file)
    output_dir = output_file[:output_file.rindex('.')]
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
    tp_backup_info = f"{output_dir}/backup_info"
    origin_backup_info = os.path.join(tp_path.parent.parent, "f_input/backup_info")
    with open(origin_backup_info, mode="rb") as f:
        with open(tp_backup_info, mode="wb+") as wf:
            data_byte = f.read()
            wf.write(data_byte)
            wf.flush()
    ssj_kbf_sqlite_convert(input_file=f"{input_dir}mymoney.sqlite", output_file=f"{output_dir}/mymoney.sqlite", convert=ConvertType.SqliteToKbf)
    out_zip = zipfile.ZipFile(output_file, 'w')
    for f in os.listdir(output_dir):
        if f == 'backup_info' or f == 'mymoney.sqlite':
            print(f"打包文件{f} --> {output_file}")
            out_zip.write(os.path.join(output_dir, f), f, zipfile.ZIP_DEFLATED)
    out_zip.close()
    pass


def convert_sw_salary_to_standard_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_data_list = []
    for f in files:
        if f.__eq__('sw_salary_person_all.csv'):
            with open(f'{input_dir}{f}', 'r', encoding='UTF-8') as rf:
                reader = csv.reader(rf)
                data_header = []
                data_read_flag = False
                for row in reader:
                    if row[0].__eq__('交易号'):
                        data_header = row
                        data_read_flag = True
                        continue
                    if data_read_flag:
                        standard_data = dict.fromkeys(standard_format_data_key)
                        column_index = 0
                        while column_index < min(len(data_header), len(standard_format_data_key)):
                            standard_data[data_header[column_index].strip()] = row[column_index].replace('\t', '').strip()
                            column_index = column_index + 1
                        standard_data_list.append(standard_data)
    print(f'sw待插入数据{len(standard_data_list)}条')
    return standard_data_list


if __name__ == '__main__':
    out_dirs = os.path.join(tp_path.parent.parent, 'f_output')
    if not os.path.exists(out_dirs):
        os.mkdir(out_dirs)
    step = list(step_chain_dict.values())
    if step[0] == 1:
        unzip_kbf()
    if step[1] == 1:
        ssj_kbf_sqlite_convert(convert=ConvertType.KbfToSqlite)
    if step[2] == 1:
        update_database_table_struct()
        account_dict, category_dict = get_config_in_database()
        convert_weixin_bill_to_standard_data_format()
        weixin_long_parse_data_list = convert_weixin_long_bill_to_standard_data_format()
        alipay_data_list = convert_alipay_bill_to_standard_data_format()
        sw_data_list = convert_sw_salary_to_standard_format()
        data_list = weixin_long_parse_data_list + alipay_data_list + sw_data_list
        ssj_data = standard_bill_file_analysis_tool(account_dict, category_dict, data_list)
        write_ssj_data_to_database(ssj_data_set=ssj_data)
    if step[3] == 1:
        if ot_file_name is None:
            ot_file_name = target_file
        ot_file = os.path.join(tp_path.parent.parent, 'f_output', ot_file_name)
        compress_file_to_kbf(output_file=ot_file)
    if step[4] == 1:
        adb_helper_clear_ssj_data(3)
    if step[5] == 1:
        adb_helper_clear_ssj_data(2)
    pass
