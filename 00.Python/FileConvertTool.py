#! /usr/bin/python3
# _*_ coding:UTF-8 _*_
import base64
import hashlib
import os
from pathlib import Path
from Crypto.Util.Padding import pad, unpad
from Crypto.Cipher import AES
from global_constant import tp_path
from global_constant import sdcard_root_path

""""
加密文件资产脚本，采用AES256的CBC模式进行加密
密钥使用32Bytes即AES256加密，初始向量IV需16Bytes
用户提供的密钥会先进行base64编码然后取MD5值32位的
BLOCK_SIZE固定为16
FILE_BLOCK_SIZE只要是16的整数倍就行，值太小会加长加解密时间
pip install pycryptodome
"""
aes_file_encrypt_key = ''
decide_list_dict = {
    'encrypt_data': 1,
    'decrypt_data': 1
}
initial_vector = ''[0:16]
origin_media_base_dir = None
encrypt_base_dir = None
decrypt_base_dir = None

BLOCK_SIZE = 16
FILE_BLOCK_SIZE = 80
USER_DEFINE_FILE_HEAD = 60
MD5_LENGTH = 32


# # 填充函数，填充模式：PKCS5Padding(填充内容等于缺少的字节数)
# pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE).encode()
# # 返填充函数
# unpad = lambda s: s[:-ord(s[len(s) - 1:])]


def get_ase_encrypt_key() -> bytes:
    base64_encode_key = base64.b64encode(aes_file_encrypt_key.encode('UTF-8'))
    md5_key = hashlib.md5(base64_encode_key)
    return md5_key.digest()


def get_file_md5(input_file_path) -> str:
    md5 = hashlib.md5()
    with open(input_file_path, 'rb') as input_f:
        for chunk in iter(lambda: input_f.read(8192), b''):
            md5.update(chunk)
    return md5.hexdigest()


def get_encrypt_file_md5_from_head(input_file_path) -> str:
    tp_f = open(input_file_path, 'rb')
    tp_f.seek(USER_DEFINE_FILE_HEAD - MD5_LENGTH)
    md5_bytes = tp_f.read(MD5_LENGTH)
    tp_f.close()
    return md5_bytes.decode("UTF-8")


def get_path_dfs_helper(path_collect_list: list, input_path: str, deep: int):
    if not os.path.exists(input_path):
        print(f'目录不存在:{input_path}')
        return
    if deep > 10:
        return
    if os.path.isfile(input_path):
        path_collect_list.append(input_path)
        return
    files = os.listdir(input_path)
    for file in files:
        f_abs = os.path.join(input_path, file)
        get_path_dfs_helper(path_collect_list, f_abs, deep + 1)
    pass


def pad_file_head(input_file: str, output_file: str):
    file_md5_str = get_file_md5(input_file)
    file_magic_number_bytes = bytes(b'\x00' * (USER_DEFINE_FILE_HEAD - MD5_LENGTH))
    with open(input_file, 'rb') as f_input:
        with open(output_file, 'wb') as f_output:
            read_file_magic_number = f_input.read(USER_DEFINE_FILE_HEAD - MD5_LENGTH)
            file_magic_list = list(file_magic_number_bytes)
            file_magic_list[:len(read_file_magic_number)] = list(read_file_magic_number)
            f_output.write(bytes(file_magic_list))
            f_output.write(file_md5_str.encode())
    pass


def encrypt_big_file_with_ase256(input_file_path: str, output_file_path: str, key: bytes):
    cipher_config = AES.new(key, mode=AES.MODE_CBC, iv=initial_vector.encode('UTF-8'))
    input_file_total_size = os.path.getsize(input_file_path)
    input_file_read_size = 0
    with open(input_file_path, 'rb') as input_f:
        with open(output_file_path, 'ab+') as output_f:
            while True:
                chunk = input_f.read(FILE_BLOCK_SIZE)
                read_size = len(chunk)
                input_file_read_size += read_size
                if read_size == 0:
                    break
                if input_file_total_size == input_file_read_size:
                    # pad last 16bytes
                    if read_size <= BLOCK_SIZE:
                        encrypt_file_block = cipher_config.encrypt(pad(chunk, BLOCK_SIZE))
                        output_f.write(encrypt_file_block)
                    else:
                        a = int(read_size / FILE_BLOCK_SIZE)
                        b = int((read_size - a * FILE_BLOCK_SIZE) / BLOCK_SIZE)
                        end = a * FILE_BLOCK_SIZE + b * BLOCK_SIZE
                        output_f.write(cipher_config.encrypt(chunk[:end]))
                        output_f.write(cipher_config.encrypt(pad(chunk[end: len(chunk)], BLOCK_SIZE)))
                        pass
                else:
                    encrypt_file_block = cipher_config.encrypt(chunk)
                    output_f.write(encrypt_file_block)
            pass
    pass


def decrypt_big_file_with_ase256(input_file: str, output_file: str, key):
    cipher_config = AES.new(key, mode=AES.MODE_CBC, iv=initial_vector.encode('UTF-8'))
    input_file_total_size = os.path.getsize(input_file)
    input_file_read_size = USER_DEFINE_FILE_HEAD
    with open(input_file, 'rb') as input_f:
        with open(output_file, 'wb+') as output_f:
            # skip file head 60 bytes which is file magic number and file md5
            input_f.seek(USER_DEFINE_FILE_HEAD)
            while True:
                file_block = input_f.read(FILE_BLOCK_SIZE)
                if file_block is None or len(file_block) <= 0:
                    break
                read_size = len(file_block)
                input_file_read_size += read_size
                if input_file_total_size == input_file_read_size:
                    a = int(read_size / FILE_BLOCK_SIZE)
                    b = int((read_size - a * FILE_BLOCK_SIZE) / BLOCK_SIZE)
                    end = a * FILE_BLOCK_SIZE + (b - 1) * BLOCK_SIZE
                    output_f.write(cipher_config.decrypt(file_block[:end]))
                    # only last 16bytes block for unpad
                    output_f.write(unpad(cipher_config.decrypt(file_block[end:len(file_block)]), BLOCK_SIZE))
                    pass
                else:
                    encrypt_file_block = cipher_config.decrypt(file_block)
                    output_f.write(encrypt_file_block)
            pass
    pass


def compare_file(file_one: str, file_two: str):
    with open(file_one, 'rb') as f_one:
        with open(file_two, 'rb') as f_two:
            while True:
                file_block_one = f_one.read(FILE_BLOCK_SIZE)
                file_block_two = f_two.read(FILE_BLOCK_SIZE)
                if file_block_one is None or len(file_block_one) <= 0:
                    break
                if file_block_two is None or len(file_block_two) <= 0:
                    break
                if len(file_block_one) != len(file_block_two):
                    print('读取block不一致')
                    break
                else:
                    data_same = True
                    for i in range(0, len(file_block_one)):
                        if file_block_one[i] != file_block_two[i]:
                            print(f'{i}处数据不一致')
                            data_same = False
                            break
                if not data_same:
                    return
    pass


if __name__ == '__main__':
    base_dir = tp_path.parent.parent
    if sdcard_root_path is not None:
        base_dir = os.path.join(sdcard_root_path, '00SERVER')
        print(f'当前为Android设备，设置处理路径为{base_dir}')
        pass
    origin_media_base_dir = os.path.join(base_dir, 'f_input', 'origin_media_data')
    if not os.path.exists(origin_media_base_dir):
        os.makedirs(origin_media_base_dir)
    encrypt_base_dir = os.path.join(base_dir, 'f_input', 'encrypt_data')
    if not os.path.exists(encrypt_base_dir):
        os.makedirs(encrypt_base_dir)
    decrypt_base_dir = os.path.join(base_dir, 'f_output', 'decrypt_data')
    if not os.path.exists(decrypt_base_dir):
        os.makedirs(decrypt_base_dir)

    decide_list = list(decide_list_dict.values())
    origin_media_file_list = []
    get_path_dfs_helper(origin_media_file_list, origin_media_base_dir, 0)

    if decide_list[0] == 1:
        for f in origin_media_file_list:
            file_name = f[f.rfind(os.path.sep) + 1:]
            output_file_abs_path = os.path.join(encrypt_base_dir, file_name)
            pad_file_head(f, output_file_abs_path)
            print(f'加密文件-->{file_name}')
            encrypt_big_file_with_ase256(f, output_file_abs_path, get_ase_encrypt_key())

    decrypt_file_list = []
    get_path_dfs_helper(decrypt_file_list, decrypt_base_dir, 0)

    encrypt_file_list = []
    get_path_dfs_helper(encrypt_file_list, encrypt_base_dir, 0)

    if decide_list[1] == 1:
        for f in encrypt_file_list:
            file_name = f[f.rfind(os.path.sep) + 1:]
            output_file_abs_path = os.path.join(decrypt_base_dir, file_name)
            file_md5_from_head = get_encrypt_file_md5_from_head(f)
            print(f'解密文件-->{file_name}')
            decrypt_big_file_with_ase256(f, output_file_abs_path, get_ase_encrypt_key())
            file_md5 = get_file_md5(output_file_abs_path)
            if file_md5_from_head != file_md5:
                print(f'{output_file_abs_path}解密后MD5不符')
        pass
    pass
