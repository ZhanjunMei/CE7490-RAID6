import copy
import logging
import os
import shutil
import random
import time
import traceback

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
        self.file_manager = FileManager(disk_size, block_size, max_file_num, disks)
        for i in range(len(disks)):
            self.file_manager.reset_disk(i)
        self.has_failed_disks = False


    def backup_test_files(self):
        backup_path = os.path.join(self.test_file_dir, '.zip')
        if not os.path.isfile(backup_path):
            shutil.make_archive(self.test_file_dir, 'zip', self.test_file_dir)


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
        self.file_manager.add_file(file_name, d0)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        d1 = self.file_manager.read_file(file_name)
        if self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1
        t2 = time.time()
        ls = set([x['file_name'] for x in self.file_manager.list_files()])
        if self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1
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
        self.file_manager.del_file(file_name)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        ls = set([x['file_name'] for x in self.file_manager.list_files()])
        if self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1
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
        self.file_manager.modify_file(file_name, begin, end, new_data)
        disk_rec_time = self.file_manager.get_recovery_time()
        if disk_rec_time is not None:
            recovery_time = disk_rec_time
            self.has_failed_disks = False
        t1 = time.time()
        d2 = self.file_manager.read_file(file_name)
        if self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1
        t2 = time.time()
        ls = self.file_manager.list_files()
        if self.file_manager.get_recovery_time() is not None:
            self.logger.info('error!')
            self.logger.info('--- recovery failed ---')
            return -1
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

                if s == 51:
                    pass

                # fail disks
                # if not self.has_failed_disks and random.random() < 0.2:
                #     if random.random() < 0.5:
                #         # 2 disks
                #         disk0 = random.randint(0, self.disk_num - 2)
                #         disk1 = random.randint(disk0 + 1, self.disk_num - 1)
                #         self.file_manager.fail_disk(disk0)
                #         self.file_manager.fail_disk(disk1)
                #     else:
                #         # 1 disk
                #         disk0 = random.randint(0, self.disk_num - 1)
                #         disk1 = -1
                #         self.file_manager.fail_disk(disk0)
                #     self.logger.info(f'fail_disk, {disk0}, {disk1}, -1, 0.0')
                #     self.has_failed_disks = True

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
                            new_len = random.randint(0, len(d0) + 3 * block_size)
                            new_data = bytearray(os.urandom(new_len))
                    res = self.test_modify_file(file_name, os_files, begin, end, new_data)
                    if res != 0:
                        break

                # corrupt block
                # if op < 1 and not self.has_failed_disks:
                #     disk_idx = random.randint(0, self.disk_num - 1)
                #     block_idx = random.randint(0, self.disk_size // self.block_size)
                #     self.test_corrupt_block(disk_idx, block_idx)

        except Exception as e:
            traceback.print_exc()
        finally:
            self.recover_test_files()


if __name__ == '__main__':
    disk_size = 1 * 500 * 1024  # Bytes
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
    ]
    myTest = Test(disk_size, block_size, max_file_num, disks)
    myTest.reset()
    myTest.random_test(steps=1000)