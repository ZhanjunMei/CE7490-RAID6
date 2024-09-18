import os
import shutil


class DiskManager:

    def __init__(self,
                 disk_size,
                 block_size,
                 disks=None,
                 ):
        self.disk_size = disk_size
        self.block_size = block_size
        self.block_num = int(disk_size // block_size)
        if disks is None:
            self.disks = [
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
            ]
        else:
            self.disks = disks
        self.disk_num = len(disks)


    def check_block(self, disk_idx, block_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
            if not os.path.isfile(block_path):
                return -2
            return 0


    def check_disk(self, disk_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return -1
            return 0


    def reset_disk(self, disk_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if os.path.exists(disk_path):
                shutil.rmtree(disk_path)
            os.makedirs(disk_path)
            for i in range(self.block_num):
                block_path = os.path.join(disk_path, 'block_{}'.format(i))
                with open(block_path, 'wb') as f:
                    f.write(b'\x00' * self.block_size)
            return 0


    def reset_block(self, disk_idx, block_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.exists(disk_path):
                return -1
            block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
            with open(block_path, 'wb') as f:
                f.write(b'\x00' * self.block_size)
            return 0


    def check_failure(self, block_idx):
        for d in range(self.disk_num):
            if self.disks[d][0] == 'f':
                disk_path = os.path.join(self.disks[d][1], 'disk_{}'.format(d))
                if not os.path.isdir(disk_path):
                    return -1
                block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
                if not os.path.isfile(block_path):
                    return -2
        return 0


    def write_block(self, block, disk_idx, block_idx, force=False):
        res = self.check_failure(block_idx)
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
            if res == 0:
                with open(block_path, 'wb') as file:
                    file.write(block)
                return 0
            if not force:
                return res
            # force to write
            if not os.path.isdir(disk_path):
                os.makedirs(disk_path)
            with open(block_path, 'wb') as file:
                file.write(block)
            return 0


    def read_block(self, disk_idx, block_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return -1, None  # disk failed
            block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
            if not os.path.isfile(block_path):
                return -2, None  # block failed
            with open(block_path, 'rb') as file:
                data = bytearray(file.read())
                if len(data) != self.block_size:
                    return -2, None  # block failed
                return 0, data


    def fail_disk(self, disk_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if os.path.exists(disk_path):
                shutil.rmtree(disk_path)
            return 0


    def corrupt_block(self, disk_idx, block_idx):
        import random
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return -1
            block_path = os.path.join(disk_path, 'block_{}'.format(block_idx))
            if not os.path.isfile(block_path):
                return -1
            with open(block_path, 'rb') as f:
                data = bytearray(f.read())
            for i in range(len(data)):
                if random.random() < 0.2:
                    data[i] = random.randint(0, 255)
            with open(block_path, 'wb') as f:
                f.write(data)
            return 0


if __name__ == '__main__':
    pass