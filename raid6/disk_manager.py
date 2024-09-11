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


    def check_disk(self, disk_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return -1
            return 0


    def clear_disk(self, disk_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if os.path.exists(disk_path):
                shutil.rmtree(disk_path)
            os.makedirs(disk_path)


    def write_block(self, block, disk_idx, block_idx):
        # Folder
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return -1  # TODO disk failed
            block_path = os.path.join(self.disks[disk_idx][1],
                                      'disk_{}'.format(disk_idx),
                                      'block_{}'.format(block_idx))
            with open(block_path, 'wb') as file:
                file.write(block)
        return 0

    def read_block(self, disk_idx, block_idx):
        if self.disks[disk_idx][0] == 'f':
            disk_path = os.path.join(self.disks[disk_idx][1], 'disk_{}'.format(disk_idx))
            if not os.path.isdir(disk_path):
                return None  # TODO disk failed
            block_path = os.path.join(self.disks[disk_idx][1],
                                      'disk_{}'.format(disk_idx),
                                      'block_{}'.format(block_idx))
            if os.path.isfile(block_path):
                with open(block_path, 'rb') as file:
                    return bytearray(file.read())
            else:
                return bytearray(b'\x00' * self.block_size)