import copy
import logging
import os
import shutil
import random
import sys
import time
import traceback
import matplotlib.pyplot as plt
from matplotlib import ticker

random.seed(0)

from raid6.file_manager import FileManager


class Test:
    def __init__(self,
                 disk_size,
                 block_size,
                 max_file_num=None,
                 disks=None):
        self.disk_size = disk_size
        self.block_size = block_size
        self.max_file_num = max_file_num
        self.disks = disks
        self.disk_num = len(disks)
        self.test_file_dir = './test_files'
        self.log_file = './test_log.txt'

        self.logger = None
        self.file_manager = None
        self.test_files = []
        self.has_failed_disks = False


    def reset(self):
        # logger
        # log format: operation, obj1, obj2, size, time
        if os.path.isfile(self.log_file):
            os.remove(self.log_file)
        self.logger = logging.getLogger('my_logger')
        self.logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        # files
        self.recover_test_files()
        self.test_files = [x for x in os.listdir(self.test_file_dir)]
        # file manager / disks
        self.file_manager = FileManager(self.disk_size, self.block_size, self.max_file_num, self.disks)
        for i in range(len(self.disks)):
            self.file_manager.reset_disk(i)
        self.has_failed_disks = False


    # zip
    def backup_test_files(self):
        backup_path = os.path.join(self.test_file_dir, '.zip')
        if not os.path.isfile(backup_path):
            shutil.make_archive(self.test_file_dir, 'zip', self.test_file_dir)


    # unzip
    def recover_test_files(self):
        backup_path = self.test_file_dir + '.zip'
        if not os.path.isfile(backup_path):
            return
        shutil.rmtree(self.test_file_dir)
        shutil.unpack_archive(backup_path, self.test_file_dir)
        os.remove(backup_path)


    def test_add_file(self, file_name, file_list):
        with open(os.path.join(self.test_file_dir, file_name), 'rb') as fread:
            d0 = fread.read()
        file_size = len(d0) if file_name not in file_list else 0
        if file_name not in file_list:
            file_list.append(file_name)

        recovery_time = None
        t0 = time.time()
        res = self.file_manager.add_file(file_name, d0)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        d1 = self.file_manager.read_file(file_name)
        if res == 0 and self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1  # writing should trigger disk check
        t2 = time.time()
        ls = set([x['file_name'] for x in self.file_manager.list_files()])
        if res == 0 and self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1  # writing should trigger disk check
        t3 = time.time()
        if set(file_list) != ls or d0 != d1:
            self.logger.info('add error!')
            self.file_manager.add_file(file_name, d0)
            self.logger.info(file_name)
            self.logger.info('--- file_list ---')
            self.logger.info(file_list)
            self.logger.info('--- ls ---')
            self.logger.info(ls)
            if d0 != d1:
                self.logger.info('--- d0 != d1 ---')
                self.logger.info(f'd0:{len(d0)}')
                self.logger.info(f'd1:{None if d1 is None else len(d1)}')
            return -1

        if recovery_time is not None:
            self.logger.info(f'recover, None, None, -1, {recovery_time:.6f}')
        self.logger.info(f'add, {file_name}, None, {file_size}, {(t1-t0):.6f}')
        self.logger.info(f'read, {file_name}, None, {len(d0)}, {(t2-t1):.6f}')
        self.logger.info(f'ls, None, None, 0, {(t3-t2):.6f}')
        return 0


    def test_delete_file(self, file_name, file_list):
        file_size = os.path.getsize(os.path.join(self.test_file_dir, file_name))
        if file_name not in file_list:
            file_size = 0
        else:
            file_list.remove(file_name)

        recovery_time = None
        t0 = time.time()
        res = self.file_manager.del_file(file_name)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        ls = set([x['file_name'] for x in self.file_manager.list_files()])
        if res == 0 and self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1  # writing should trigger disk check
        t2 = time.time()
        if set(file_list) != ls:
            self.logger.info('delete error!')
            self.logger.info(file_name)
            self.logger.info('--- file_list ---')
            self.logger.info(file_list)
            self.logger.info('--- ls ---')
            self.logger.info(ls)
            return -1

        if recovery_time is not None:
            self.logger.info(f'recover, None, None, -1, {recovery_time:.6f}')
        self.logger.info(f'delete, {file_name}, None, {file_size}, {(t1 - t0):.6f}')
        self.logger.info(f'ls, None, None, 0, {(t2 - t1):.6f}')
        return 0


    def test_read_file(self, file_name, file_list):
        d0 = None
        if file_name in file_list:
            with open(os.path.join(self.test_file_dir, file_name), 'rb') as f:
                d0 = f.read()

        recovery_time = None
        t0 = time.time()
        d1 = self.file_manager.read_file(file_name)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        if d0 != d1:
            self.logger.info('read error!')
            self.logger.info(file_name)
            self.logger.info('--- file_size ---')
            self.logger.info(f'd0: {-1 if d0 is None else len(d0)}')
            self.logger.info(f'd1: {-1 if d1 is None else len(d1)}')
            return -1

        if recovery_time is not None:
            self.logger.info(f'recover, None, None, -1, {recovery_time:.6f}')
        self.logger.info(f'read, {file_name}, None, {0 if d0 is None else len(d0)}, {(t1-t0):.6f}')
        return 0


    def test_modify_file(self, file_name, file_list, begin, end, new_data):
        d0, d1, d2 = None, None, None
        if file_name in file_list:
            with open(os.path.join(self.test_file_dir, file_name), 'rb') as f:
                d0 = f.read()
            d1 = bytearray()
            d1.extend(d0[0:begin])
            d1.extend(new_data)
            d1.extend(d0[end:])
            with open(os.path.join(self.test_file_dir, file_name), 'wb') as f:
                f.write(d1)
            file_size = len(d0) if len(d0) == len(d1) else len(d0) + len(d1)
        else:
            file_size = 0

        recovery_time = None
        t0 = time.time()
        res = self.file_manager.modify_file(file_name, begin, end, new_data)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        d2 = self.file_manager.read_file(file_name)
        if res == 0 and self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1  # writing should trigger disk check
        t2 = time.time()
        ls = self.file_manager.list_files()
        if res == 0 and self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1  # writing should trigger disk check
        t3 = time.time()
        if d1 != d2:
            self.logger.info('modify error!')
            self.logger.info(file_name)
            self.logger.info('--- file_list ---')
            self.logger.info(file_list)
            self.logger.info('--- ls ---')
            for e in ls:
                self.logger.info(e)
            self.logger.info('--- begin, end, file_len, new_len ---')
            self.logger.info(f'{begin}, {end}, {0 if d1 is None else len(d1)}, {len(new_data)}')
            return -1

        if recovery_time is not None:
            self.logger.info(f'recover, None, None, 0, {recovery_time:.6f}')
        self.logger.info(f'modify, {file_name}, None, {file_size}, {(t1 - t0):.6f}')
        self.logger.info(f'read, {file_name}, None, {file_size}, {(t2 - t1):.6f}')
        self.logger.info(f'ls, None, None, 0, {(t3-t2):.6f}')
        return 0


    def fail_disks(self, disk_list):
        if self.has_failed_disks:
            return
        for d in disk_list:
            self.file_manager.fail_disk(d)
        self.has_failed_disks = True


    def test_corrupt_block(self, disk_idx, block_idx):
        if self.has_failed_disks:
            return
        self.file_manager.corrupt_block(disk_idx, block_idx)
        t0 = time.time()
        self.file_manager.check_and_recover_corruption(block_idx)
        t1 = time.time()
        self.logger.info(f'recover_c, {disk_idx}, {block_idx}, -1, {(t1 - t0):.6f}')


    def random_test(self, steps):
        try:
            self.backup_test_files()
            os_files = []
            out_files = copy.deepcopy(self.test_files)
            for s in range(steps):
                op = random.random() # operation id

                # fail disks
                if not self.has_failed_disks and random.random() < 0.2:
                    if random.random() < 0.5:
                        # 2 disks
                        disk0 = random.randint(0, self.disk_num - 2)
                        disk1 = random.randint(disk0 + 1, self.disk_num - 1)
                        self.file_manager.fail_disk(disk0)
                        self.file_manager.fail_disk(disk1)
                    else:
                        # 1 disk
                        disk0 = random.randint(0, self.disk_num - 1)
                        disk1 = -1
                        self.file_manager.fail_disk(disk0)
                    self.logger.info(f'fail_disk, {disk0}, {disk1}, -1, 0.0')
                    self.has_failed_disks = True

                self.logger.info(f'--- s:{s}, op:{op:.4f}, failed:{self.has_failed_disks} ---')

                # add file
                if op < 0.2:
                    if len(out_files) == 0:
                        file_name = os_files[random.randint(0, len(os_files) - 1)]
                    else:
                        if random.random() < 0.8 or len(os_files) == 0:
                            file_name = out_files[random.randint(0, len(out_files) - 1)]
                        else:
                            file_name = os_files[random.randint(0, len(os_files) - 1)]
                    if file_name in out_files:
                        out_files.remove(file_name)
                    res = self.test_add_file(file_name, os_files)
                    if res != 0:
                        break

                # delete file
                elif op < 0.4:
                    if len(os_files) == 0:
                        file_name = out_files[random.randint(0, len(out_files) - 1)]
                    else:
                        if random.random() < 0.8 or len(out_files) == 0:
                            file_name = os_files[random.randint(0, len(os_files) - 1)]
                        else:
                            file_name = out_files[random.randint(0, len(out_files) - 1)]
                    if file_name not in out_files:
                        out_files.append(file_name)
                    res = self.test_delete_file(file_name, os_files)
                    if res != 0:
                        break

                # read file
                elif op < 0.6:
                    if len(os_files) == 0:
                        file_name = out_files[random.randint(0, len(out_files) - 1)]
                    else:
                        if random.random() < 0.8 or len(out_files) == 0:
                            file_name = os_files[random.randint(0, len(os_files) - 1)]
                        else:
                            file_name = out_files[random.randint(0, len(out_files) - 1)]
                    res = self.test_read_file(file_name, os_files)
                    if res != 0:
                        break

                # modify file
                elif op < 0.8:
                    if len(os_files) == 0:
                        file_name = out_files[random.randint(0, len(out_files) - 1)]
                    else:
                        if random.random() < 0.8 or len(out_files) == 0:
                            file_name = os_files[random.randint(0, len(os_files) - 1)]
                        else:
                            file_name = out_files[random.randint(0, len(out_files) - 1)]

                    begin, end, d1 = 10, 20, None
                    new_data = bytearray(os.urandom(10))
                    if file_name in os_files:
                        with open(os.path.join(self.test_file_dir, file_name), 'rb') as fm:
                            d0 = fm.read()
                        t = random.random()
                        if t < 0.1:
                            begin = end = random.randint(0, len(d0))
                        elif t < 0.3:
                            if random.random() < 0.5:
                                begin, end = 0, random.randint(0, len(d0))
                            else:
                                begin, end = random.randint(0, len(d0)), len(d0)
                        else:
                            begin = random.randint(0, len(d0))
                            end = random.randint(begin, len(d0))
                        if random.random() < 0.7:
                            new_data = bytearray(os.urandom(end - begin))
                        else:
                            new_len = random.randint(0, len(d0) + 3 * self.block_size)
                            new_data = bytearray(os.urandom(new_len))
                    res = self.test_modify_file(file_name, os_files, begin, end, new_data)
                    if res != 0:
                        break

                # corrupt block
                elif op < 1 and not self.has_failed_disks:
                    disk_idx = random.randint(0, self.disk_num - 1)
                    block_idx = random.randint(0, int(self.disk_size // self.block_size) - 1)
                    self.test_corrupt_block(disk_idx, block_idx)

        except Exception as e:
            traceback.print_exc()
        finally:
            self.recover_test_files()


def random_test():
    disk_size = 1 * 512 * 1024  # Bytes
    block_size = 4 * 1024  # Bytes
    max_file_num = 10
    disks = [
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
        ('f', './disks/'),
    ]
    myTest = Test(disk_size, block_size, max_file_num, disks)
    myTest.reset()
    myTest.random_test(steps=1000)


def time_test1():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    modify_begin, modify_end = 10 * 1024, 30 * 1024
    modify_data = bytearray(os.urandom(modify_end - modify_begin))
    block_sizes = [1, 2, 4, 8, 16, 32, 64]
    disk_nums = [4, 8, 12]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'add': [[] for _ in range(len(disk_nums))],
        'read': [[] for _ in range(len(disk_nums))],
        'modify': [[] for _ in range(len(disk_nums))],
        'delete': [[] for _ in range(len(disk_nums))],
    }
    for di, disk_num in enumerate(disk_nums):
        print(f'--- disk: {disk_num} ---')
        disks = [('f', './disks/')] * disk_num
        block_sizes = block_sizes
        for b in block_sizes:
            block_size = b * 1024
            print(f'----- block: {block_size} ------')
            file_manager = FileManager(disk_size, block_size, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            t0 = time.time()
            file_manager.add_file('1.jpg', d0)
            t1 = time.time()
            d1 = file_manager.read_file('1.jpg')
            t2 = time.time()
            file_manager.modify_file('1.jpg', modify_begin, modify_end, modify_data)
            t3_1 = time.time()
            d2 = file_manager.read_file('1.jpg')
            t3_2 = time.time()
            file_manager.del_file('1.jpg')
            t4 = time.time()

            if d1 != d0:
                print('--- error in reading ---')
                sys.exit()
            dm = bytearray(copy.deepcopy(d0))
            dm[modify_begin:modify_end] = modify_data
            if d2 != dm:
                print('--- error in modifying ---')
                sys.exit()
            if file_manager.read_file('1.jpg') is not None:
                print('--- error in deleting ---')
                sys.exit()

            print(f'add: {(t1 - t0):.6f}')
            print(f'read: {(t2 - t1):.6f}')
            print(f'modify: {(t3_1 - t2):.6f}')
            print(f'delete: {(t4 - t3_2):.6f}')
            result['add'][di].append(t1 - t0)
            result['read'][di].append(t2 - t1)
            result['modify'][di].append(t3_1 - t2)
            result['delete'][di].append(t4 - t3_2)

    for op in ['add', 'read', 'modify', 'delete']:
        file_path = f'./test_time/test_block_{op}.jpg'
        plt.clf()
        for di, d in enumerate(result['disk_nums']):
            plt.plot(result['block_sizes'], result[op][di], label=f'disk_{d}')
        plt.title(f'{op} time')
        plt.xlabel('block size (KB)')
        plt.ylabel('time (s)')
        plt.legend()
        plt.savefig(file_path)


def time_test2():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    modify_begin, modify_end = 10 * 1024, 30 * 1024
    modify_data = bytearray(os.urandom(modify_end - modify_begin))
    block_sizes = [4, 16, 64]
    disk_nums = [4, 5, 6, 7, 8, 9, 10, 11, 12]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'add': [[] for _ in range(len(block_sizes))],
        'read': [[] for _ in range(len(block_sizes))],
        'modify': [[] for _ in range(len(block_sizes))],
        'delete': [[] for _ in range(len(block_sizes))],
    }
    for bi, b in enumerate(block_sizes):
        block_size = b * 1024
        print(f'--- block: {block_size} ---')
        for di, disk_num in enumerate(disk_nums):
            disks = [('f', './disks/')] * disk_num
            print(f'--- disk: {disk_num} ---')
            file_manager = FileManager(disk_size, block_size, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            t0 = time.time()
            file_manager.add_file('1.jpg', d0)
            t1 = time.time()
            d1 = file_manager.read_file('1.jpg')
            t2 = time.time()
            file_manager.modify_file('1.jpg', modify_begin, modify_end, modify_data)
            t3_1 = time.time()
            d2 = file_manager.read_file('1.jpg')
            t3_2 = time.time()
            file_manager.del_file('1.jpg')
            t4 = time.time()
            if d1 != d0:
                print('--- error in reading ---')
                sys.exit()
            dm = bytearray(copy.deepcopy(d0))
            dm[modify_begin:modify_end] = modify_data
            if d2 != dm:
                print('--- error in modifying ---')
                sys.exit()
            if file_manager.read_file('1.jpg') is not None:
                print('--- error in deleting ---')
                sys.exit()

            print(f'add: {(t1 - t0):.6f}')
            print(f'read: {(t2 - t1):.6f}')
            print(f'modify: {(t3_1 - t2):.6f}')
            print(f'delete: {(t4 - t3_2):.6f}')
            result['add'][bi].append(t1 - t0)
            result['read'][bi].append(t2 - t1)
            result['modify'][bi].append(t3_1 - t2)
            result['delete'][bi].append(t4 - t3_2)

    for op in ['add', 'read', 'modify', 'delete']:
        file_path = f'./test_time/test_disk_{op}.jpg'
        plt.clf()
        for bi, b in enumerate(result['block_sizes']):
            plt.plot(result['disk_nums'], result[op][bi], label=f'block_{b}KB')
        plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        plt.title(f'{op} time')
        plt.xlabel('disk number')
        plt.ylabel('time (s)')
        plt.legend()
        plt.savefig(file_path)


def time_test3():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    block_sizes = [1, 2, 4, 8, 16, 32, 64]
    disk_nums = [4, 6, 8]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'recover_1': [[] for _ in range(len(disk_nums))],
        'recover_2': [[] for _ in range(len(disk_nums))],
    }
    for di, disk_num in enumerate(disk_nums):
        print('disk num', disk_num)
        for bi, block_size in enumerate(block_sizes):
            print('block', block_size)
            disks = [('f', './disks/')] * disk_num
            file_manager = FileManager(disk_size, block_size * 1024, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            file_manager.add_file('1.jpg', d0)
            file_manager.fail_disk(0)
            file_manager.del_file('1.jpg')  # to trigger recovery
            t1 = file_manager.get_recovery_time()

            file_manager.add_file('1.jpg', d0)
            file_manager.fail_disk(0)
            file_manager.fail_disk(1)
            file_manager.del_file('1.jpg')  # to trigger recovery
            t2 = file_manager.get_recovery_time()

            result['recover_1'][di].append(t1)
            result['recover_2'][di].append(t2)

    for i, op in enumerate(['recover_1', 'recover_2']):
        file_path = f'./test_time/test_block_fail_{i+1}.jpg'
        plt.clf()
        for di, d in enumerate(result['disk_nums']):
            plt.plot(result['block_sizes'], result[op][di], label=f'disk_{d}')
        plt.title(f'recover time from {i+1} failure')
        plt.xlabel('block size (KB)')
        plt.ylabel('time (s)')
        plt.legend()
        plt.savefig(file_path)


def time_test4():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    block_sizes = [4, 16, 64]
    disk_nums = [4, 5, 6, 7, 8]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'recover_1': [[] for _ in range(len(block_sizes))],
        'recover_2': [[] for _ in range(len(block_sizes))],
    }
    for bi, block_size in enumerate(block_sizes):
        print('block_size', block_size)
        for di, disk_num in enumerate(disk_nums):
            print('disk', disk_num)
            disks = [('f', './disks/')] * disk_num
            file_manager = FileManager(disk_size, block_size * 1024, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            file_manager.add_file('1.jpg', d0)
            file_manager.fail_disk(0)
            file_manager.del_file('1.jpg')  # to trigger recovery
            t1 = file_manager.get_recovery_time()

            file_manager.add_file('1.jpg', d0)
            file_manager.fail_disk(0)
            file_manager.fail_disk(1)
            file_manager.del_file('1.jpg')  # to trigger recovery
            t2 = file_manager.get_recovery_time()

            result['recover_1'][bi].append(t1)
            result['recover_2'][bi].append(t2)

    for i, op in enumerate(['recover_1', 'recover_2']):
        file_path = f'./test_time/test_disk_fail_{i+1}.jpg'
        plt.clf()
        for bi, b in enumerate(result['block_sizes']):
            plt.plot(result['disk_nums'], result[op][bi], label=f'block_{b}KB')
        plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
        plt.title(f'recover time from {i+1} failure')
        plt.xlabel('disk number')
        plt.ylabel('time (s)')
        plt.legend()
        plt.savefig(file_path)


def time_test5():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    block_sizes = [1, 2, 4, 8, 16, 32, 64]
    disk_nums = [4, 6, 8]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'recover': [[] for _ in range(len(disk_nums))],
    }
    for di, disk_num in enumerate(disk_nums):
        print('disk num', disk_num)
        for bi, block_size in enumerate(block_sizes):
            print('block', block_size)
            disks = [('f', './disks/')] * disk_num
            file_manager = FileManager(disk_size, block_size * 1024, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            file_manager.add_file('1.jpg', d0)
            t0 = time.time()
            file_manager.check_and_recover_corruption(0)
            t1 = time.time()
            result['recover'][di].append(t1 - t0)

    file_path = f'./test_time/test_block_corrupt.jpg'
    plt.clf()
    for di, d in enumerate(result['disk_nums']):
        plt.plot(result['block_sizes'], result['recover'][di], label=f'disk_{d}')
    plt.title(f'recover time from 1 corruption')
    plt.xlabel('block size (KB)')
    plt.ylabel('time (s)')
    plt.legend()
    plt.savefig(file_path)


def time_test6():
    disk_size = 1 * 1024 * 1024  # Bytes
    max_file_num = 1
    with open('./test_time/1.jpg', 'rb') as f:
        d0 = f.read()
    block_sizes = [4, 16, 64]
    disk_nums = [4, 5, 6, 7, 8]
    result = {
        'block_sizes': block_sizes,
        'disk_nums': disk_nums,
        'recover': [[] for _ in range(len(block_sizes))],
    }
    for bi, block_size in enumerate(block_sizes):
        print('block_size', block_size)
        for di, disk_num in enumerate(disk_nums):
            print('disk', disk_num)
            disks = [('f', './disks/')] * disk_num
            file_manager = FileManager(disk_size, block_size * 1024, max_file_num, disks)
            for d in range(disk_num):
                file_manager.reset_disk(d)
            file_manager.add_file('1.jpg', d0)
            t0 = time.time()
            file_manager.check_and_recover_corruption(0)
            t1 = time.time()
            result['recover'][bi].append(t1 - t0)

    file_path = f'./test_time/test_disk_corrupt.jpg'
    plt.clf()
    for bi, b in enumerate(result['block_sizes']):
        plt.plot(result['disk_nums'], result['recover'][bi], label=f'block_{b}KB')
    plt.gca().xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    plt.title(f'recover time from 1 corruption')
    plt.xlabel('disk number')
    plt.ylabel('time (s)')
    plt.legend()
    plt.savefig(file_path)


# extreme test
def test0():
    disk_size = 1 * 2 * 1024  # Bytes
    block_size = 1 * 1024  # Bytes
    max_file_num = 2
    disks = [('f', './disks/')] * 257
    myTest = Test(disk_size, block_size, max_file_num, disks)
    myTest.reset()
    file_manager = myTest.file_manager

    with open('test_extreme/1.png', 'rb') as f:
        d01 = f.read()
    with open('test_extreme/2.png', 'rb') as f:
        d02 = f.read()

    file_manager.add_file('1.png', d01)
    file_manager.add_file('2.png', d02)
    if file_manager.read_file('1.png') != d01:
        print('--- read error 1 ---')
        sys.exit()
    if file_manager.read_file('2.png') != d02:
        print('--- read error 2 ---')
        sys.exit()
    file_manager.fail_disk(0)
    file_manager.fail_disk(256)
    file_manager.del_file('2.png')
    if file_manager.read_file('1.png') != d01:
        print('--- recovery error 1 ---')
        sys.exit()
    myTest.test_corrupt_block(123, 0)
    if file_manager.read_file('1.png') != d01:
        print('--- recovery error 2 ---')
        sys.exit()


if __name__ == '__main__':
    pass
    # extreme test
    # test0()

    # random test
    random_test()

    # timing tests
    # time_test1()
    # time_test2()
    # time_test3()
    # time_test4()
    # time_test5()
    # time_test6()
