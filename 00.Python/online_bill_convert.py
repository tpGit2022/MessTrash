#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
import csv
import os.path
import re
import shutil
import pdfminer
import pdfplumber
import global_constant
from comm_tools import convert_to_timestamp
from comm_tools import remove_blank_newline_tab
from comm_tools import write_list_dict_values_to_csv_file
from global_constant import ImportDataSource
from global_constant import bank_pdf_password_list
from global_constant import origin_bill_dir
from global_constant import standard_format_data_key
from global_constant import tp_path

"""
本脚本用于辅助处理随手记数据导入，将支付宝，微信，银行账单数据转化为标准账单数据用于随手记数据导入
基本处理逻辑：
1.每一个数据来源形成一个单独的原始依据时间升序数据csv文件,同时生成当前数据源新增数据的临时文件
2. 所有数据源的临时文件经过去重写入汇总文件
3. 所有处理完的账单文件转移到origin_bill_data文件夹下
微信有两种类型账单
1. 用于个人对账用的账单 时间间隔最大3个月 导出的格式是csv
2. 用作证明材料的账单 时间间隔最大一年 导出的pdf 借助pdfplumber库可直接提取表单内容

支付宝导出对时间没有限制 导出的格式是csv
中国银行导出的账单 时间间隔最长6个月，导出的格式 pdf,有密码  借助pdfplumber库可直接提取表单内容
本脚本将会把不同来源数据分别整入csv文件
支付宝账单 alipay_bill_origin_data.csv
微信账单  weixin_bill_origin_data.csv
中国银行  china_bank_bill_origin_data.csv
pip install pdfplumber
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pdfplumber
"""

included_bill_data_dict = {}
standard_bill_data_list_need_insert = []


def get_update_dict_set_status(dict_set: dict, key: str, value: str):
    if not dict_set.keys().__contains__(key):
        dict_set[f'{key}'] = set()
        return False
    elif dict_set[key].__contains__(value):
        return True
    else:
        dict_set[key].add(value)
        return False


def convert_weixin_long_bill_data_from_pdf_to_standard_data_format():
    """
    借助pdfplumber解析微信长账单pdf格式，可直接提取表单数据
    :return:
    """
    input_dirs = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dirs)
    standard_weixin_bill_data_list = []
    for f in files:
        pdf_file_path = os.path.join(input_dirs, f)
        if f.startswith('微信支付交易明细证明') and f.lower().endswith('.pdf'):
            print(f'开始处理微信文件-->{pdf_file_path}')
            pdf = None
            try:
                pdf = pdfplumber.open(pdf_file_path)
                real_data_flag = False
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            if row[0].strip().__eq__('交易单号'):
                                real_data_flag = True
                                continue
                            if not real_data_flag:
                                continue
                            trade_id = remove_blank_newline_tab(row[0])
                            if get_update_dict_set_status(included_bill_data_dict, ImportDataSource.weixin.value, trade_id):
                                # 重复数据
                                if global_constant.print_repeat_data_info:
                                    print(f'微信长账单重复数据 trade_id:{trade_id} --> {row}')
                                continue
                            standard_weixin_bill_data = dict.fromkeys(standard_format_data_key)
                            standard_weixin_bill_data['交易号'] = trade_id + '\t'
                            standard_weixin_bill_data['商家订单号'] = remove_blank_newline_tab(row[7]) + '\t'
                            trade_time = row[1].replace('\n', ' ').strip()
                            standard_weixin_bill_data['交易创建时间'] = trade_time
                            standard_weixin_bill_data['付款时间'] = standard_weixin_bill_data['交易创建时间']
                            standard_weixin_bill_data['最近修改时间'] = standard_weixin_bill_data['交易创建时间']
                            standard_weixin_bill_data['交易对方'] = remove_blank_newline_tab(row[6])
                            standard_weixin_bill_data['商品名称'] = ' '
                            standard_weixin_bill_data['金额'] = float(row[5])
                            standard_weixin_bill_data['收/支'] = remove_blank_newline_tab(row[3])
                            standard_weixin_bill_data['交易状态'] = '交易成功'
                            if row[4].__contains__('银行'):
                                standard_weixin_bill_data['交易方式'] = '银行卡'
                            else:
                                standard_weixin_bill_data['交易方式'] = '微信钱包'
                            standard_weixin_bill_data['资金状态'] = ''
                            standard_weixin_bill_data['数据来源'] = ImportDataSource.weixin.value
                            standard_weixin_bill_data['时间戳'] = convert_to_timestamp(trade_time)
                            standard_weixin_bill_data['备注'] = f"交易类型:{remove_blank_newline_tab(row[2])}\n数据来源:微信长账单导入"
                            standard_weixin_bill_data_list.append(standard_weixin_bill_data)
            finally:
                if pdf is not None:
                    pdf.close()
            if global_constant.move_origin_data:
                if os.path.exists(os.path.join(origin_bill_dir, f)):
                    print(f'该表单已存在同名文件!!!,请手动核验删除{f}')
                else:
                    print(f"开始移动{pdf_file_path} --> {origin_bill_dir}")
                    shutil.move(pdf_file_path, origin_bill_dir)
            pass
    if len(standard_weixin_bill_data_list) <= 0:
        print('微信长账单无新增数据')
        return standard_weixin_bill_data_list
    print(f'微信长账单新增数据{len(standard_weixin_bill_data_list)}条')
    standard_weixin_bill_data_list.sort(key=lambda cmp_data: int(cmp_data['时间戳']))
    # 追加写入微信账单全文件
    remain_weixin_all_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                    global_constant.remain_weixin_bill_standard_format_data_file_name)
    print(f'追加写入文件{remain_weixin_all_bill_data_file}')
    write_list_dict_values_to_csv_file(remain_weixin_all_bill_data_file, 'a', standard_weixin_bill_data_list)
    # 新建临时微信账单新增文件
    tp_weixin_new_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                global_constant.tp_prefix + global_constant.remain_weixin_bill_standard_format_data_file_name)
    print(f'新建微信账单新增临时文件{tp_weixin_new_bill_data_file}')
    write_list_dict_values_to_csv_file(tp_weixin_new_bill_data_file, 'a+', standard_weixin_bill_data_list)
    return standard_weixin_bill_data_list


def convert_alipay_bill_from_csv_to_standard_data_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_alipay_data_list = []
    for f in files:
        if f.startswith("alipay_record") and f.endswith(".csv"):
            csv_file_path = os.path.join(input_dir, f)
            print(f'开始处理-->{csv_file_path}')
            with open(csv_file_path, 'r', encoding='GBK') as rf:
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
                        trade_id = standard_data['交易号'].replace('\t', '').strip()
                        if get_update_dict_set_status(included_bill_data_dict, ImportDataSource.alipay.value, trade_id):
                            if global_constant.print_repeat_data_info:
                                print(f'支付宝重复数据{trade_id}-->{row}')
                            continue
                        standard_data['金额'] = standard_data['金额（元）']
                        standard_data.pop('金额（元）')
                        standard_data.pop('交易来源地')
                        standard_data.pop('类型')
                        standard_data.pop('成功退款（元）')
                        standard_data['交易方式'] = '支付宝'
                        if standard_data['资金状态'] is None:
                            standard_data['资金状态'] = ' '
                        if standard_data['备注'] is None:
                            standard_data['备注'] = ' '
                        standard_data['备注'] = f'{standard_data["备注"]}\n数据来源:支付宝'
                        standard_data['数据来源'] = ImportDataSource.alipay.value
                        standard_data['时间戳'] = convert_to_timestamp(standard_data['交易创建时间'])
                        standard_alipay_data_list.append(standard_data)
            if global_constant.move_origin_data:
                if os.path.exists(os.path.join(origin_bill_dir, f)):
                    print(f'该表单已存在同名文件!!!,请手动核验删除{f}')
                else:
                    print(f"移动{csv_file_path} --> {origin_bill_dir}")
                    shutil.move(csv_file_path, origin_bill_dir)
    if len(standard_alipay_data_list) <= 0:
        print('支付宝无新增数据')
        return
    print(f'支付宝账单新增数据{len(standard_alipay_data_list)}条')
    standard_alipay_data_list.sort(key=lambda cmp_data: int(cmp_data['时间戳']))
    # 追加写入支付宝账单全文件
    remain_alipay_all_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                    global_constant.remain_alipay_bill_standard_format_data_file_name)
    print(f'追加写入文件{remain_alipay_all_bill_data_file}')
    write_list_dict_values_to_csv_file(remain_alipay_all_bill_data_file, 'a', standard_alipay_data_list)
    # 新建临时支付宝账单新增文件
    tp_alipay_new_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                global_constant.tp_prefix + global_constant.remain_alipay_bill_standard_format_data_file_name)
    print(f'新建支付宝账单新增临时文件{tp_alipay_new_bill_data_file}')
    write_list_dict_values_to_csv_file(tp_alipay_new_bill_data_file, 'w+', standard_alipay_data_list)
    return standard_alipay_data_list


def convert_weixin_short_bill_from_csv_to_standard_data_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_weixin_short_bill_data_list = []
    for f in files:
        if f.startswith("微信支付账单") and f.endswith(".csv"):
            weixin_csv_bill_file = os.path.join(input_dir, f)
            print(f'开始处理-->{weixin_csv_bill_file}')
            with open(weixin_csv_bill_file, 'r', encoding="UTF-8") as rf:
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
                            standard_data[data_header[column_index].strip()] = row[column_index].replace('\t',
                                                                                                         '').strip()
                            column_index = column_index + 1
                        standard_data['金额'] = standard_data['金额(元)'].replace('¥', '')
                        standard_data['交易创建时间'] = standard_data['交易时间']
                        standard_data['付款时间'] = standard_data['交易时间']
                        standard_data['最近修改时间'] = standard_data['交易时间']
                        standard_data['商品名称'] = standard_data['商品']
                        standard_data['交易号'] = standard_data['交易单号']
                        trade_id = standard_data['交易号'].replace('\t', '').strip()
                        if get_update_dict_set_status(included_bill_data_dict, ImportDataSource.weixin.value, trade_id):
                            if global_constant.print_repeat_data_info:
                                print(f'微信短账单重复数据{trade_id}-->{row}')
                            continue
                        standard_data['商家订单号'] = standard_data['商户单号']
                        standard_data['交易状态'] = '交易成功'
                        if standard_data['收/支'].__contains__('/') or standard_data['交易对方'].__contains__('理财通') or \
                                standard_data['交易对方'].__contains__('债券') or standard_data['交易对方'].__contains__('基金销售') or standard_data['商品名称'].__contains__('理财通') or \
                                standard_data['商品名称'].__contains__('基金销售') or standard_data['商品名称'].__contains__('债券'):
                            standard_data['收/支'] = '其他'
                            standard_data['资金状态'] = '资金转移'
                            if standard_data['交易类型'].__contains__('零钱通') and standard_data['商品名称'].strip().__eq__('/'):
                                standard_data['商品名称'] = '微信-零钱通-基金销售'

                        pay_channel = standard_data['支付方式']
                        if pay_channel.__contains__('银行'):
                            standard_data['交易方式'] = '银行卡'
                        else:
                            standard_data['交易方式'] = '微信钱包'

                        if standard_data['资金状态'] is None:
                            standard_data['资金状态'] = ' '
                        if standard_data['备注'] is None:
                            standard_data['备注'] = ' '
                        if standard_data['交易类型'] is not None:
                            standard_data['备注'] = f"{standard_data['备注']}\n交易类型:{standard_data['交易类型']}\n"

                        standard_data['备注'] = standard_data['备注'].replace('/', '')
                        standard_data['备注'] = f'{standard_data["备注"]}{standard_data["当前状态"]}数据来源:微信短账单'
                        standard_data['数据来源'] = ImportDataSource.weixin.value
                        standard_data['时间戳'] = convert_to_timestamp(standard_data['交易创建时间'])
                        standard_data.pop('交易时间')
                        standard_data.pop('商品')
                        standard_data.pop('金额(元)')
                        standard_data.pop('交易类型')
                        standard_data.pop('交易单号')
                        standard_data.pop('商户单号')
                        standard_data.pop('支付方式')
                        standard_data.pop('当前状态')
                        standard_weixin_short_bill_data_list.append(standard_data)
            if global_constant.move_origin_data:
                if os.path.exists(os.path.join(origin_bill_dir, f)):
                    print(f'该表单已存在同名文件!!!,请手动核验删除{f}')
                else:
                    print(f'移动{weixin_csv_bill_file} --> {origin_bill_dir}')
                    shutil.move(weixin_csv_bill_file, origin_bill_dir)
    if len(standard_weixin_short_bill_data_list) <= 0:
        print(f'微信短账单无新增数据')
        return
    print(f'微信短账单新增数据{len(standard_weixin_short_bill_data_list)}条')
    standard_weixin_short_bill_data_list.sort(key=lambda cmp_data: int(cmp_data['时间戳']))
    # 追加写入微信账单全文件
    remain_weixin_all_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                    global_constant.remain_weixin_bill_standard_format_data_file_name)
    print(f'追加写入文件{remain_weixin_all_bill_data_file}')
    write_list_dict_values_to_csv_file(remain_weixin_all_bill_data_file, 'a', standard_weixin_short_bill_data_list)
    # 新建临时微信账单新增文件
    tp_weixin_new_bill_data_file = os.path.join(tp_path.parent.parent, 'f_input',
                                                global_constant.tp_prefix + global_constant.remain_weixin_bill_standard_format_data_file_name)
    print(f'新建微信账单新增临时文件{tp_weixin_new_bill_data_file}')
    write_list_dict_values_to_csv_file(tp_weixin_new_bill_data_file, 'a+', standard_weixin_short_bill_data_list)
    return standard_weixin_short_bill_data_list


def convert_sw_salary_from_csv_to_standard_format():
    input_dir = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dir)
    standard_data_list = []
    for f in files:
        if f.__eq__(global_constant.remain_sw_person_all_data_file_name):
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


def convert_china_bank_bill_data_from_pdf_to_standard_format():
    pattern = re.compile('[0-9]*.pdf')
    input_dirs = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dirs)
    china_bank_standard_bill_data_list = []
    for f in files:
        if re.match(pattern, f):
            pdf_file_path = os.path.join(input_dirs, f)
            print(f'开始处理中国银行账单-->{pdf_file_path}')
            pdf = None
            for password in bank_pdf_password_list:
                try:
                    pdf = pdfplumber.open(pdf_file_path, password=password)
                    print(f'correct password {pdf_file_path} --> {password}')
                    data_read_flag = False
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                if row[0].__contains__('记账日期'):
                                    data_read_flag = True
                                    continue
                                if not data_read_flag:
                                    continue
                                # 记账日期0 记账时间1 币别2 金额3 余额4 交易名称5 渠道6 网点名称7 附言8 对方账户名9 对方卡号10 对方开户行11
                                trade_time = f"{row[0]} {row[1]}"
                                bill_data = dict.fromkeys(standard_format_data_key)
                                time_stamp = convert_to_timestamp(trade_time)
                                bill_data['交易号'] = f"china_bank_{time_stamp * 1000}\t"
                                trade_id = bill_data['交易号'].replace('\t', '')
                                if get_update_dict_set_status(included_bill_data_dict, ImportDataSource.china_bank.value, trade_id):
                                    # 重复数据
                                    if global_constant.print_repeat_data_info:
                                        print(f'中国银行账单重复数据{trade_id} --> {row}')
                                    continue
                                bill_data['时间戳'] = time_stamp
                                bill_data['交易创建时间'] = trade_time
                                bill_data['付款时间'] = bill_data['交易创建时间']
                                bill_data['最近修改时间'] = bill_data['交易创建时间']
                                bill_data['交易对方'] = remove_blank_newline_tab(row[9])
                                if bill_data['交易对方'].__contains__("----"):
                                    bill_data['交易对方'] = '未知交易方'
                                bill_data['交易对方'] = remove_blank_newline_tab(bill_data['交易对方'])
                                bill_data['商品名称'] = ' \t'
                                amount = float(row[3].replace(',', ''))
                                bill_data['金额'] = abs(amount)
                                bill_data['收/支'] = "其他"
                                bill_data['交易状态'] = '交易成功'
                                bill_data['交易方式'] = '银行卡'
                                if amount > 0:
                                    bill_data['备注'] = '\n银行账单-账户收入\n'
                                else:
                                    bill_data['备注'] = '\n银行账单-账户支出\n'
                                if not row[4].__contains__("---"):
                                    bill_data['备注'] = bill_data['备注'] + f"账户当前余额:{row[4]}\n"
                                if not row[5].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"交易名称:{remove_blank_newline_tab(row[5])}\n"
                                if not row[9].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"对方账户名:{remove_blank_newline_tab(row[9])}\n"
                                if not row[10].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"对方卡/账号:{remove_blank_newline_tab(row[10])}\n"
                                if not row[11].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"对方开户行:{remove_blank_newline_tab(row[11])}\n"
                                if not row[6].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"渠道:{remove_blank_newline_tab(row[6])}\n"
                                if not row[7].__contains__('---'):
                                    bill_data['备注'] = bill_data['备注'] + f"网点名称:{remove_blank_newline_tab(row[7])}\n"
                                if not row[8].__contains__('----'):
                                    bill_data['备注'] = bill_data['备注'] + f"附言:{remove_blank_newline_tab(row[8])}\n"
                                bill_data['备注'] = bill_data['备注'].strip() + "\n数据来源:银行账单解析"
                                bill_data['数据来源'] = ImportDataSource.china_bank.value
                                if bill_data['交易对方'].__contains__('微粒贷') or bill_data['交易对方'].__contains__('微众银行') or \
                                        bill_data['交易对方'].__contains__('度小满') or bill_data['交易对方'].__contains__('福州三六零') \
                                        or bill_data['交易对方'].__contains__('招联消费') or bill_data['交易对方'].__contains__('招联金融') \
                                        or bill_data['交易对方'].__contains__('重庆苏宁小额') or bill_data['交易对方'].__contains__('苏宁易付宝') \
                                        or bill_data['交易对方'].__eq__('银联代收付二次清算款') \
                                        or bill_data['交易对方'].__contains__('京东金融') or bill_data['交易对方'].__contains__('网银在线（北京）科技'):
                                    bill_data['资金状态'] = '资金转移'
                                china_bank_standard_bill_data_list.append(bill_data)
                except pdfminer.pdfdocument.PDFPasswordIncorrect:
                    pass
                    # PDFPasswordIncorrect
                    # print(f"password error {pdf_file_path} -->{password}")
                finally:
                    if pdf is not None:
                        pdf.close()
                    pass
            if global_constant.move_origin_data:
                if os.path.exists(os.path.join(origin_bill_dir, f)):
                    print(f'该表单已存在同名文件!!!,请手动核验删除{f}')
                else:
                    print(f"开始移动{pdf_file_path} -->{origin_bill_dir}")
                    shutil.move(pdf_file_path, origin_bill_dir)
        pass
    if len(china_bank_standard_bill_data_list) <= 0:
        print('中国银行无新增账单数据')
        return china_bank_standard_bill_data_list
    print(f"中国银行账单数据{len(china_bank_standard_bill_data_list)}条")
    china_bank_standard_bill_data_list.sort(key=lambda cmp_data: int(cmp_data['时间戳']))
    bank_bill_file_name = os.path.join(tp_path.parent.parent, 'f_input', global_constant.remain_china_bank_bill_standard_format_data_file_name)
    print('追加写入中国银行账单留存文件')
    write_list_dict_values_to_csv_file(bank_bill_file_name, 'a', china_bank_standard_bill_data_list)
    print('写入中国银行临时新增文件')
    tp_bank_bill_file = os.path.join(tp_path.parent.parent, 'f_input', global_constant.tp_prefix + global_constant.remain_china_bank_bill_standard_format_data_file_name)
    write_list_dict_values_to_csv_file(tp_bank_bill_file, 'w', china_bank_standard_bill_data_list)
    return china_bank_standard_bill_data_list


def extract_china_bank_bill_data_from_pdf():
    pattern = re.compile('[0-9]*.pdf')
    input_dirs = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dirs)
    bank_bill_data_list = []
    trade_name_set = set()
    trade_piple_set = set()
    trade_account_name_set = set()
    for f in files:
        if re.match(pattern, f):
            pdf_file_path = os.path.join(input_dirs, f)
            print(f'开始处理中国银行账单-->{pdf_file_path}')
            pdf = None
            first_file = True
            row_header = None
            for password in bank_pdf_password_list:
                try:
                    pdf = pdfplumber.open(pdf_file_path, password=password)
                    print(f'correct password {pdf_file_path} --> {password}')
                    data_read_flag = False
                    for page in pdf.pages:
                        tables = page.extract_tables()
                        for table in tables:
                            for row in table:
                                # 记账日期0 记账时间1 币别2 金额3 余额4 交易名称5 渠道6 网点名称7 附言8 对方账户名9 对方卡号10 对方开户行11
                                if row[0].__contains__('记账日期'):
                                    data_read_flag = True
                                    if first_file:
                                        first_file = False
                                        row_header = row
                                    continue
                                if not data_read_flag:
                                    continue
                                # 记账日期0 记账时间1 币别2 金额3 余额4 交易名称5 渠道6 网点名称7 附言8 对方账户名9 对方卡号10 对方开户行11
                                trade_time = f"{row[0]} {row[1]}"
                                trade_name_set.add(row[5])
                                trade_piple_set.add(row[6])
                                if not (row[9].__contains__('财付通') or row[9].__contains__('支付宝') or row[9].__contains__('京东商城')):
                                    trade_account_name_set.add(row[9])
                                time_stamp = convert_to_timestamp(trade_time)
                                row.append(time_stamp)
                                bank_bill_data_list.append(row)
                except pdfminer.pdfdocument.PDFPasswordIncorrect:
                    pass
                    # PDFPasswordIncorrect
                    # print(f"password error {pdf_file_path} -->{password}")
                finally:
                    if pdf is not None:
                        pdf.close()
                    pass
    print('交易名称')
    print(trade_name_set)
    print('渠道')
    print(trade_piple_set)
    print('账户名')
    print(trade_account_name_set)
    if len(bank_bill_data_list) <= 0:
        print('无银行账单数据')
        return
    bank_bill_data_list.sort(key=lambda cmp_data: int(cmp_data[12]))
    china_bank_origin_csv_file_name = 'origin_china_bank_bill_data.csv'
    china_bank_origin_csv_file_abs_path = os.path.join(input_dirs, china_bank_origin_csv_file_name)
    with open(china_bank_origin_csv_file_abs_path, mode='w+', encoding='UTF-8', newline='') as csv_file:
        csv_write = csv.writer(csv_file)
        if row_header is not None:
            csv_write.writerow(row_header)
        for r in bank_bill_data_list:
            csv_write.writerow(r)


def load_data_for_remove_duplicate_by_file_name(input_file):
    global included_bill_data_dict
    file_abs_path = os.path.join(tp_path.parent.parent, 'f_input', input_file)
    if not os.path.exists(file_abs_path):
        print(f'文件不存在,新建文件{file_abs_path}')
        with open(file_abs_path, mode='w', encoding='UTF-8', newline='') as f:
            csv_write = csv.writer(f)
            csv_write.writerow(standard_format_data_key)
        return
    with open(file_abs_path, encoding='UTF-8', mode='r') as f:
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
            key = standard_data['数据来源']
            if not included_bill_data_dict.keys().__contains__(key):
                included_bill_data_dict[f'{key}'] = set()
            value = included_bill_data_dict[f"{key}"]
            data_no = standard_data['交易号'].strip()
            if not value.__contains__(data_no):
                included_bill_data_dict[f"{key}"].add(data_no)
            else:
                if global_constant.print_repeat_data_info:
                    print(f'存在重复数据{key}-->{data_no}')


def collection_all_data_to_csv():
    """
    整合所有数据源的数据
    :return:
    """
    global included_bill_data_dict
    input_dirs = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dirs)
    import_standard_bill_data_list = []
    included_bill_data_dict.clear()
    load_data_for_remove_duplicate_by_file_name(global_constant.all_online_data_to_import_file_name)
    for f in files:
        if (f.startswith(global_constant.remain_prefix) and f.endswith('.csv')) or f.__eq__(global_constant.remain_sw_person_all_data_file_name):
            f_abs = os.path.join(input_dirs, f)
            with open(f_abs, mode='r', encoding='UTF-8') as csv_f:
                csv_reader = csv.reader(csv_f)
                for row in csv_reader:
                    if row[0].__eq__('交易号'):
                        data_header = row
                        data_read_flag = True
                        continue
                    if data_read_flag:
                        data_source = row[13].strip()
                        trade_id = row[0].replace('\t', '').strip()
                        if get_update_dict_set_status(included_bill_data_dict, data_source, trade_id):
                            if global_constant.print_repeat_data_info:
                                print(f'import data 重复数据{trade_id}-->{row}')
                            continue
                        standard_data = dict.fromkeys(standard_format_data_key)
                        column_index = 0
                        while column_index < min(len(data_header), len(standard_format_data_key)):
                            standard_data[data_header[column_index].strip()] = row[column_index].replace('\t', '').strip()
                            column_index = column_index + 1
                        # 如何剔除微信和支付中支付渠道是银行卡的账单 避免账单的二次导入
                        if data_source.__eq__(ImportDataSource.china_bank.value):
                            if standard_data['交易对方'].__contains__('支付宝') or standard_data['交易对方'].__contains__('财付通') or standard_data['交易对方'].__contains__('京东商城'):
                                continue
                            pass
                        # if data_source.__eq__(ImportDataSource.china_bank)
                        import_standard_bill_data_list.append(standard_data)
    if len(import_standard_bill_data_list) <= 0:
        print('所有数据源均无待新增账单数据')
        return
    print(f"共计新增账单数据{len(import_standard_bill_data_list)}条")
    import_standard_bill_data_list.sort(key=lambda cmp_data: int(cmp_data['时间戳']))
    import_all_bill_file_name = os.path.join(tp_path.parent.parent, 'f_input', global_constant.all_online_data_to_import_file_name)
    print('追加写入总账单留存文件')
    write_list_dict_values_to_csv_file(import_all_bill_file_name, 'a', import_standard_bill_data_list)
    print('写入总临时新增文件')
    tp_bank_bill_file = os.path.join(tp_path.parent.parent, 'f_input', global_constant.new_add_online_data_from_all_data_source)
    with open(tp_bank_bill_file, mode='w+', encoding='UTF-8', newline='') as cf:
        csv_write = csv.writer(cf)
        csv_write.writerow(standard_format_data_key)
        for row in import_standard_bill_data_list:
            csv_write.writerow(list(row.values()))


def init_config():
    if not os.path.exists(origin_bill_dir):
        os.makedirs(origin_bill_dir)
    need_load_file_name_tuple = (global_constant.remain_weixin_bill_standard_format_data_file_name,
                                 global_constant.remain_alipay_bill_standard_format_data_file_name,
                                 global_constant.remain_china_bank_bill_standard_format_data_file_name,
                                 global_constant.remain_sw_person_all_data_file_name)
    for i in need_load_file_name_tuple:
        load_data_for_remove_duplicate_by_file_name(i)


def clean_temp_file():
    input_dirs = os.path.join(tp_path.parent.parent, 'f_input/')
    files = os.listdir(input_dirs)
    for f in files:
        if f.startswith('tp_remain_bill_data') and f.endswith('.csv'):
            f_abs = os.path.join(input_dirs, f)
            print(f'删除临时文件-->{f_abs}')
            os.remove(f_abs)


def convert_all_online_bill_data_to_standard():
    global standard_bill_data_list_need_insert
    init_config()
    print('开始处理长微信账单....')
    convert_weixin_long_bill_data_from_pdf_to_standard_data_format()

    print('开始处理微信短账单.....')
    convert_weixin_short_bill_from_csv_to_standard_data_format()

    print('开始处理支付宝账单.....')
    convert_alipay_bill_from_csv_to_standard_data_format()

    print('开始处理银行账单.....')
    convert_china_bank_bill_data_from_pdf_to_standard_format()

    print('开始处理私人账单.....')
    convert_sw_salary_from_csv_to_standard_format()

    print('数据汇总处理.....')
    collection_all_data_to_csv()

    print('删除临时文件.......')
    clean_temp_file()
    pass


if __name__ == '__main__':
    convert_all_online_bill_data_to_standard()
    pass
