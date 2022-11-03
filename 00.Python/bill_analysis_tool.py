#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
"""
随手记电子账单导入工具
"""
import os.path
import sqlite3
import zipfile
import csv
import time
import shutil
import copy
import datetime
import global_constant
from global_constant import tp_path
from global_constant import new_unique_id
from global_constant import ConvertType
from global_constant import TradeType
from global_constant import standard_format_data_key
from ssj_decide_category import decide_category
# from online_bill_convert import convert_all_online_bill_data_to_standard
from online_bill_convert_without_pdf_lib import convert_all_online_bill_data_to_standard

tp_kbf_file_name = ''
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


def unzip_kbf(input_files=None):
    global tp_kbf_file_name
    if input_files is None:
        if global_constant.full_update or global_constant.sdcard_root_path is None:
            input_file_abs_path = os.path.join(tp_path.parent.parent, 'f_input/')
        else:
            input_file_abs_path = os.path.join(global_constant.sdcard_root_path, '.mymoney', 'backup/')
    else:
        if global_constant.full_update or global_constant.sdcard_root_path is None:
            input_file_abs_path = os.path.join(tp_path.parent.parent, 'f_input/', input_files)
        else:
            input_file_abs_path = os.path.join(global_constant.sdcard_root_path, '.mymoney', 'backup/', input_files)
    if not os.path.exists(input_file_abs_path):
        print(f'{input_file_abs_path}不存在')
        raise FileNotFoundError(f'输入文件kbf不存在-->{input_file_abs_path}')
    print(f'输入待解压kbf文件路径:{input_file_abs_path}')
    if os.path.isdir(input_file_abs_path):
        files = os.listdir(input_file_abs_path)
        for f in files:
            if f.endswith('.kbf'):
                global tp_kbf_file_name
                tp_kbf_file_name = f
                zf = zipfile.ZipFile(os.path.join(input_file_abs_path, f))
                zf.extractall(path=os.path.join(tp_path.parent.parent, 'f_input/'))
                break
    elif os.path.isfile(input_file_abs_path):
        if input_files.lower().endswith('.kbf'):
            tp_kbf_file_name = input_files
            zf = zipfile.ZipFile(input_file_abs_path)
            zf.extractall(path=os.path.join(tp_path.parent.parent, 'f_input/'))
        pass


def get_config_in_database(sqlite3_path=None):
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')
    if global_constant.print_repeat_data_info:
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

    # print(account_info_dict)
    # print()
    # print(category_info_dict)
    query_ret = """select transactionPOID from t_transaction ORDER By t_transaction.transactionPOID DESC limit 1 ;"""
    ret = cur.execute(query_ret)
    t = ret.fetchone()
    if t is not None:
        last_transaction_poid = t[0]
        if last_transaction_poid < 0:
            last_transaction_poid = -last_transaction_poid
        global_constant.unique_id = last_transaction_poid + 1
        print(f'修正初始unique_id为{global_constant.unique_id}')
    cur.close()
    conn.close()
    return account_info_dict, category_info_dict


def read_standard_bill_data_from_file():
    standard_bill_data_list = []
    all_standard_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input', global_constant.all_online_data_to_import_file_name)
    if not os.path.exists(all_standard_bill_data_file):
        print(f'文件{all_standard_bill_data_file}不存在!!!')
        return
    with open(all_standard_bill_data_file, mode='r', encoding='UTF-8'):
        with open(all_standard_bill_data_file, encoding='UTF-8', mode='r') as f:
            reader = csv.reader(f)
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
                standard_bill_data_list.append(standard_data)
        pass
    return standard_bill_data_list


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
        if standard_data['交易创建时间'] is None or len(standard_data['交易创建时间']) == 0 or len(
                standard_data['交易创建时间'].strip()) == 0:
            ssj_data_model['createdTime'] = standard_data['交易创建时间']
        else:
            time_array = time.strptime(standard_data['交易创建时间'].strip(), time_format)
            timestamp = int(time.mktime(time_array)) * 1000
            ssj_data_model['createdTime'] = timestamp

        if standard_data['最近修改时间'] is None or len(standard_data['最近修改时间']) == 0 or len(
                standard_data['最近修改时间'].strip()) == 0:
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

        ssj_data_model[
            'memo'] = f"交易对方:{standard_data['交易对方']}\n商品名称:{standard_data['商品名称']}\n收/支:{standard_data['收/支']}\n交易状态:{standard_data['交易状态']}\n" \
                      f"交易方式:{standard_data['交易方式']}\n资金状态:{standard_data['资金状态']}\n交易时间:{standard_data['交易创建时间']}\n交易号:{standard_data['交易号']}\n商家订单号:{standard_data['商家订单号']}\n备注:{standard_data['备注']}"
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
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data,
                                        ssj_data_list)
            ssj_data_model['type'] = TradeType.Income.value
            ssj_data_model['buyerAccountPOID'] = 0
            ssj_data_model['sellerAccountPOID'] = account_info_dict[f"{standard_data['交易方式']}"]
            ssj_data_model['sellerCategoryPOID'] = 0
            if ssj_data_model['buyerCategoryPOID'] is None or (
                    isinstance(ssj_data_model['buyerCategoryPOID'], int) and ssj_data_model['buyerCategoryPOID'] == 0):
                ssj_data_model['buyerCategoryPOID'] = category_info_dict['未分类收入']
                ssj_data_model['memo'] = f"{ssj_data_model['memo']}\n纳入未分类收入"
        elif standard_data['收/支'].__eq__('支出'):
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data,
                                        ssj_data_list)
            ssj_data_model['type'] = TradeType.Expense.value
            ssj_data_model['buyerAccountPOID'] = account_info_dict[f"{standard_data['交易方式']}"]
            ssj_data_model['buyerCategoryPOID'] = 0
            ssj_data_model['sellerAccountPOID'] = 0
            if ssj_data_model['sellerCategoryPOID'] is None or (isinstance(ssj_data_model['sellerCategoryPOID'], int) and ssj_data_model['sellerCategoryPOID'] == 0):
                ssj_data_model['sellerCategoryPOID'] = category_info_dict['其他支出']
                ssj_data_model['memo'] = f"{ssj_data_model['memo']}\n纳入未分类支出"
        else:
            need_jump = decide_category(ssj_data_model, account_info_dict, category_info_dict, standard_data,
                                        ssj_data_list)
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
    if global_constant.print_repeat_data_info:
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
    if global_constant.print_repeat_data_info:
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
        return
        pass
    if operation == 2:
        # push file
        os.system(r"adb shell rm -rf /sdcard/.mymoney/backup/")
        os.system(r"adb shell mkdir /sdcard/.mymoney/backup/")
        pass
    if operation == 3:
        os.system(r"adb shell su -c ls -al /data/data/com.mymoney/databases/")
        os.system(r"adb shell su -c pm clear com.mymoney")
        os.system(r"adb shell su -c ls -al /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c rm -rf /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c mkdir /sdcard/.mymoney/backup/")
        os.system(r"adb shell su -c ls -al /sdcard/.mymoney/backup/")


def update_database_table_struct(sqlite3_path=None):
    print('更新数据库字段等信息.....')
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')
    if global_constant.print_repeat_data_info:
        print(f"数据库路径:{sqlite3_path}")
    conn = sqlite3.connect(sqlite3_path)
    cur = conn.cursor()

    # add field
    field_need_insert_tuple = ('transaction_number', 'merchant_order_number', 'import_data_source')
    for field_data in field_need_insert_tuple:
        query_ret = f"select * from sqlite_master where name='t_transaction' and sql like '%{field_data}%';"
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            if global_constant.print_repeat_data_info:
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

    account_need_insert_tuple = ('花呗', '借呗', '招商金融', '度小满金融', '京东金融', '苏宁金融', '微粒贷', '微信钱包', '未知账户')
    for account_name in account_need_insert_tuple:
        query_ret = f"select * from t_account WHERE name == '{account_name}'"
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            if global_constant.print_repeat_data_info:
                print(f"表 t_account 数据，需新增账户 --> {account_name}")
            account_tuple_tp = (
            -account_id_seed, account_name, '-3', '1663640017823', '0', '9', '0', 'CNY', '', '0', '0', '0', '0', '0',
            -account_id_seed, '', 'zhang_hu_xuni', '0')
            account_id_seed = account_id_seed + 1
            insert_account_data.append(account_tuple_tp)
    cur.executemany(insert_account_sql, insert_account_data)
    conn.commit()
    query_ret = """select accountPOID,name from t_account;"""
    ret = cur.execute(query_ret)
    for row in ret:
        if global_constant.print_repeat_data_info:
            print(row)

    update_id_seed_sql = f"UPDATE t_id_seed set idSeed = {account_id_seed} WHERE tableName == 't_account'"
    cur.execute(update_id_seed_sql)
    conn.commit()
    query_ret = """select * from t_id_seed WHERE t_id_seed.tableName == 't_account'"""
    ret = cur.execute(query_ret)
    id_seed_tuple = ret.fetchone()
    if global_constant.print_repeat_data_info:
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
    category_need_insert_data_dict = {'福彩': ('0', '休闲娱乐'), '体彩': ('0', '休闲娱乐'), '食材': ('0', '食品酒水'),
                                      '保险费': ('0', '金融保险'),
                                      '未分类支出': ('0', '其他杂项'), '贷款借入': ('1', '其他收入'), '未分类收入': ('1', '其他收入')}
    for item in category_need_insert_data_dict.items():
        key = item[0]
        value = item[1]
        query_ret = f"select * from t_category where  t_category.name == '{key}'"
        ret = cur.execute(query_ret)
        if len(list(ret)) == 0:
            if global_constant.print_repeat_data_info:
                print(f"表 t_category 数据，需新增类型 --> {key}")
            category_key = value[1]
            category_tuple_tp = (-category_id_seed, key, category_first_level_dict[f'{category_key}'][0],
                                 f"{category_first_level_dict[f'{category_key}'][3]}{-category_id_seed}/", '2',
                                 '1663639743742', '-3', 'defaultIcon', '0', value[0], '0', -category_id_seed,
                                 'icon_qtzx', '0')
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


def check_data_legality(input_data_list: list):
    """
    检测将导入数据是否合法，主要检测两点
    1. type=0即支出类型时, buyerAccountPOID 或者 sellerCategoryPOID 等于0为非法数据
    2. type=1即收入类型时 sellerAccountPOID 或者 buyerCategoryPOID 等于0为非法数据
    3. 转账类型type=2和type=3应当成对出现
    4. 同一条转账记录relation应当一致 FSourceKey不一致
    :return:
    """
    copy_list = copy.copy(input_data_list)
    for i in range(len(copy_list) - 1, -1, -1):
        if copy_list[i]['type'] == 0 and (copy_list[i]['buyerAccountPOID'] == 0 or copy_list[i]['sellerCategoryPOID'] == 0):
            print(f'支出数据非法:{copy_list[i]}')
            return False
        if copy_list[i]['type'] == 1 and (copy_list[i]['sellerAccountPOID'] == 0 or copy_list[i]['buyerCategoryPOID'] == 0):
            print(f'收入数据非法:{copy_list[i]}')
            return False
        if copy_list[i]['type'] == 2 or copy_list[i]['type'] == 3:
            continue
        copy_list.pop(i)
    copy_list.sort(key=lambda cmp_data: int(cmp_data['createdTime']))
    order_index = (-2, -1, 0, 1, 2)
    for i in range(0, len(copy_list) - 1, 2):
        if copy_list[i]['createdTime'] != copy_list[i + 1]['createdTime']:
            # 抛出上两个和下两个
            print('转账数据异常，将打印异常数据上下两条...')
            for j in order_index:
                if 0 <= i + j < len(copy_list):
                    print(f'索引{j}:{i + j}-->{copy_list[i + j]}')
            return False
        if copy_list[i]['relation'] != copy_list[i + 1]['relation']:
            print('转账数据异常relation不一致，将打印异常数据上下两条...')
            for j in order_index:
                if 0 <= i + j < len(copy_list):
                    print(f'索引{j}:{i + j}-->{copy_list[i + j]}')
            return False
    return True
    pass


def write_ssj_data_to_database(sqlite3_path=None, ssj_data_set=None):
    if sqlite3_path is not None and not os.path.exists(sqlite3_path):
        print(f"数据库文件不存在:{sqlite3_path}")
        return
    if sqlite3_path is None:
        sqlite3_path = os.path.join(tp_path.parent.parent, 'f_output/mymoney.sqlite')

    if global_constant.print_repeat_data_info:
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
    ssj_kbf_sqlite_convert(input_file=f"{input_dir}mymoney.sqlite", output_file=f"{output_dir}/mymoney.sqlite",
                           convert=ConvertType.SqliteToKbf)
    out_zip = zipfile.ZipFile(output_file, 'w')
    for f in os.listdir(output_dir):
        if f == 'backup_info' or f == 'mymoney.sqlite':
            print(f"打包文件{f} --> {output_file}")
            out_zip.write(os.path.join(output_dir, f), f, zipfile.ZIP_DEFLATED)
    out_zip.close()
    pass


def update_input_config_if_android_devices():
    """
    如果当前设备为 Android 设备则更新输入配置
    当未指定输入的文件名或者指定的文件名不存在
    处于实际情况考虑重设input_file只考虑包含当前日期的kbf文件
    :return:
    """
    if global_constant.sdcard_root_path is None:
        return
    print('当前为Android设备，更新输入配置中....')
    if global_constant.input_kbf_file_name is not None and len(global_constant.input_kbf_file_name.strip()) > 0:
        kbf_abs_path = os.path.join(global_constant.sdcard_root_path, '.mymoney', 'backup', global_constant.input_kbf_file_name)
        if not os.path.exists(kbf_abs_path):
            raise FileNotFoundError(f'指定的kbf文件不存在!!!-->{kbf_abs_path}')
    else:
        print('未指定有效kbf文件名,搜索有效kbf中...')
        now_time = datetime.datetime.now()
        now_time_str = datetime.datetime.strftime(now_time, '%Y%m%d')
        kbf_abs_dir = os.path.join(global_constant.sdcard_root_path, '.mymoney', 'backup')
        if not os.path.exists(kbf_abs_dir):
            raise FileNotFoundError(f'随手记数据备份目录不存在!!!-->{kbf_abs_dir}')
        kbf_files = os.listdir(kbf_abs_dir)
        flag_valid_file = False
        for k in kbf_files:
            # print(f'随手记kbf-->{os.path.join(kbf_abs_dir, k)}')
            if k.__contains__(now_time_str) and (k.__contains__('默认账本') or k.__contains__('流水核验')):
                flag_valid_file = True
                global_constant.input_kbf_file_name = k
                print(f'重设输入kbf文件为-->{global_constant.input_kbf_file_name}')
            elif k.__contains__(now_time_str):
                print(f'移除冗余备份文件-->{os.path.join(kbf_abs_dir, k)}')
                os.remove(os.path.join(kbf_abs_dir, k))
        if not flag_valid_file:
            raise FileNotFoundError('未找到符合要求的kbf文件')
    pass


def copy_kbf_to_ssj_backup_dir_after_convert():
    if global_constant.sdcard_root_path is None:
        return
    print('当前为Android设备,拷贝转化后的kbf文件至随手记的数据备份目录....')
    source_kbf_file = os.path.join(tp_path.parent.parent, 'f_output', global_constant.output_kbf_file_name)
    des_kbf_dir = os.path.join(global_constant.sdcard_root_path, '.mymoney', 'backup', global_constant.output_kbf_file_name)
    print(f'源文件:{source_kbf_file}')
    print(f'目标文件:{des_kbf_dir}')
    shutil.copyfile(source_kbf_file, des_kbf_dir)


if __name__ == '__main__':
    out_dirs = os.path.join(tp_path.parent.parent, 'f_output')
    update_input_config_if_android_devices()
    if not os.path.exists(out_dirs):
        os.makedirs(out_dirs)
    if global_constant.step_chain_dict['unzip_kbf_file'] == 1:
        unzip_kbf(global_constant.input_kbf_file_name)
    if global_constant.step_chain_dict['convert_from_kbf_to_sqlite'] == 1:
        ssj_kbf_sqlite_convert(convert=ConvertType.KbfToSqlite)
    if global_constant.step_chain_dict['convert_bill_file']:
        convert_all_online_bill_data_to_standard()
    if global_constant.step_chain_dict['update_db_config']:
        update_database_table_struct()
        account_dict, category_dict = get_config_in_database()

    if global_constant.step_chain_dict['load_need_insert_data']:
        data_list = read_standard_bill_data_from_file()
        ssj_data = standard_bill_file_analysis_tool(account_dict, category_dict, data_list)
        if check_data_legality(ssj_data):
            write_ssj_data_to_database(ssj_data_set=ssj_data)
        else:
            print('数据不合法!!!')
            raise ValueError('待写入数据不合法!!!')

    if global_constant.step_chain_dict['compress_all_file_to_kbf']:
        if global_constant.output_kbf_file_name is None:
            global_constant.output_kbf_file_name = tp_kbf_file_name
        ot_file = os.path.join(tp_path.parent.parent, 'f_output', global_constant.output_kbf_file_name)
        # os.path.join(tp_path.parent.parent, 'f_output', output_kbf_file_name)
        compress_file_to_kbf(output_file=ot_file)

    copy_kbf_to_ssj_backup_dir_after_convert()
    pass
