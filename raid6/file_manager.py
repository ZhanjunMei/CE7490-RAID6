from raid6.disk_manager import DiskManager


class FileManager:

    """
    block_format:
    [0:4]: size_in_this_block
    [4:8]: next_disk_idx
    [8:12]: next_block_idx
    [...]: data
    
    file_table_format:
    [0:20]: file_name
    [20:24]: file_size
    [24:28]: disk_idx
    [28:32]: block_idx
    """

    def __init__(self,
                 disk_size,
                 block_size,
                 max_file_num=None,
                 disks=None,
                 ):
        if disks is None:
            disks = [
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
                ('f', './disks/'),
            ]
        self.disk_num = len(disks)
        self.disk_size = disk_size
        self.block_size = block_size
        self.block_num = disk_size // block_size
        self.block_head_size = 12
        self.block_data_size = self.block_size - self.block_head_size
        # disk_manager
        self.disk_manager = DiskManager(disk_size, block_size, disks)
        # file_table
        self._max_file_blocks = 0
        self._last_table_disk = 0
        self._last_table_block = 0
        self._table_entry_size = 32
        self._init_file_table(max_file_num)

    def _init_file_table(self, max_files):
        max_file_num = (self.disk_num - 2) * (self.disk_size // self.block_size)
        if max_files is not None:
            max_file_num = min(max_files, max_file_num)
        max_entry_size = max_file_num * self._table_entry_size
        max_table_blocks = max_entry_size // self.block_size
        if max_entry_size % self.block_size != 0:
            max_table_blocks += 1
        self._max_file_blocks = (self.disk_num - 2) * (self.disk_size // self.block_size) - max_table_blocks
        res = max_table_blocks % (self.disk_num - 2)
        self._last_table_block = max_table_blocks // (self.disk_num - 2)
        if res == 0:
            self._last_table_disk = self.disk_size - 1
        else:
            block_idx = max_table_blocks // (self.disk_num - 2)
            self._last_table_block = block_idx
            p_idx = self._get_p_disk(block_idx)
            q_idx = self._get_q_disk(block_idx)
            disk_idx = 0
            while res > 0:
                if disk_idx == p_idx or disk_idx == q_idx:
                    disk_idx += 1
                else:
                    disk_idx += 1
                    res -= 1
            self._last_table_disk = disk_idx - 1


    def _get_p_disk(self, block_idx):
        return (block_idx + self.disk_num - 2) % self.disk_num


    def _get_q_disk(self, block_idx):
        return (block_idx + self.disk_num - 1) % self.disk_num


    def _block_get_size(self, b_block):
        return int.from_bytes(b_block[0:4], 'little')


    def _block_get_next_disk(self, b_block):
        return int.from_bytes(b_block[4:8], 'little')


    def _block_get_next_block(self, b_block):
        return int.from_bytes(b_block[8:12], 'little')
    

    def _block_get_data(self, b_block, size=None):
        if size is None:
            size = self._block_get_size(b_block)
        return b_block[12:12+size]


    def _next_available_block(self, this_disk, this_block):
        d, b = this_disk, this_block
        while True:
            d += 1
            if d >= self.disk_num:
                d = 0
                b += 1
            if b >= self.block_num:
                return None
            if self._get_p_disk(b) == d or self._get_q_disk(b) == d:
                continue
            block = self._read_block(d, b)
            size = self._block_get_size(block)
            if size == 0:
                return d, b


    def _entry_byte_to_dict(self, b_entry, entry_disk, entry_block, entry_offset):
        null_idx = b_entry.find(b'\x00')
        if null_idx == 0:
            name = None
        elif null_idx == -1:
            name = b_entry[0:20].decode()
        else:
            name = b_entry[0:null_idx].decode()
        size = int.from_bytes(b_entry[20:24], byteorder='little')
        disk = int.from_bytes(b_entry[24:28], byteorder='little')
        block = int.from_bytes(b_entry[28:32], byteorder='little')
        return {
            'entry_disk': entry_disk,
            'entry_block': entry_block,
            'entry_offset': entry_offset,
            'file_name': name,
            'file_size': size,
            'file_disk': disk,
            'file_block': block
        }


    def _get_file_entry(self, file_name):
        d, b = -1, 0
        while True:
            d += 1
            if d >= self.disk_num:
                d = 0
                b += 1
            if self._get_p_disk(b) == d or self._get_q_disk(b) == d:
                continue
            block = self._read_block(d, b)
            offset = 0
            while offset < self.block_size:
                entry = self._entry_byte_to_dict(
                    block[offset:offset+self._table_entry_size], d, b, offset)
                if entry['file_name'] is None:
                    offset += self._table_entry_size
                    continue
                if file_name == entry['file_name']:
                    return entry
                offset += self._table_entry_size
            if d == self._last_table_disk and b == self._last_table_block:
                return None


    def _add_file_to_table(self, file_name, file_size, file_disk, file_block):
        d, b = -1, 0
        while d != self._last_table_disk or b != self._last_table_block:
            d += 1
            if d >= self.disk_num:
                d = 0
                b += 1
            if self._get_p_disk(b) == d or self._get_q_disk(b) == d:
                continue
            block = self._read_block(d, b)
            offset = 0
            while offset < len(block):
                if block[offset] != 0x0:
                    offset += self._table_entry_size
                    continue
                entry = bytearray(str(file_name).encode('utf-8'))
                if len(entry) < 20:
                    entry.extend([0] * (20 - len(entry)))
                entry.extend(file_size.to_bytes(4, 'little'))
                entry.extend(file_disk.to_bytes(4, 'little'))
                entry.extend(file_block.to_bytes(4, 'little'))
                block[offset:offset+self._table_entry_size] = entry
                self._write_block(block, d, b)
                self._reset_pq(b)
                return 0
        return -1


    def _del_file_from_table(self, file_entry):
        d, b = file_entry['entry_disk'], file_entry['entry_block']
        block = self._read_block(d, b)
        offset = file_entry['entry_offset']
        block[offset:offset+self._table_entry_size] = bytearray(b'\x00' * self._table_entry_size)
        self._write_block(block, d, b)
        self._reset_pq(b)


    def _read_block(self, disk_idx, block_idx):
        return self.disk_manager.read_block(disk_idx, block_idx)


    def _write_block(self, block, disk_idx, block_idx):
        return self.disk_manager.write_block(block, disk_idx, block_idx)


    def _cal_block_p(self, block_idx):
        blocks = []
        for i in range(self.disk_num):
            if i != self._get_p_disk(block_idx) and i != self._get_q_disk(block_idx):
                blocks.append(self._read_block(i, block_idx))
        block_p = bytearray()
        for j in range(self.block_size):
            tmp = [blocks[i][j] for i in range(len(blocks))]
            p = 0x11  # TODO
            block_p.append(p)
        return block_p


    def _cal_block_q(self, block_idx):
        blocks = []
        for i in range(self.disk_num):
            if i != self._get_p_disk(block_idx) and i != self._get_q_disk(block_idx):
                blocks.append(self._read_block(i, block_idx))
        block_q = bytearray()
        for j in range(self.block_size):
            tmp = [blocks[i][j] for i in range(len(blocks))]
            q = 0x12  # TODO
            block_q.append(q)
        return block_q

    def _reset_pq(self, block_idx):
        block_p = self._cal_block_p(block_idx)
        self._write_block(block_p, self._get_p_disk(block_idx), block_idx)
        block_q = self._cal_block_q(block_idx)
        self._write_block(block_q, self._get_q_disk(block_idx), block_idx)

    def _able_to_add_file(self, file_name, file_size):
        entries = self.list_files()
        occupied_blocks = 0
        for e in entries:
            if e['file_name'] == file_name:
                return -2
            blocks = e['file_size'] // self.block_size
            if blocks * self.block_size != e['file_size']:
                blocks += 1
            occupied_blocks += blocks
        new_blocks = file_size // self.block_size
        if new_blocks * self.block_size != file_size:
            new_blocks += 1
        if occupied_blocks + new_blocks > self._max_file_blocks:
            return -1
        return 0


    def _able_to_modify_file(self, file_name, begin, end, new_size):
        if begin > end:
            return -1, None
        entries = self.list_files()
        occupied_blocks = 0
        entry = None
        for e in entries:
            if e['file_name'] == file_name:
                if begin < 0 or begin > e['file_size'] or end < 0 or end > e['file_size']:
                    return -1, None
                size_change = new_size - (end - begin)
                if size_change == 0:
                    return 0, e
                f_size = e['file_size'] + size_change
                entry = e
            else:
                f_size = e['file_size']
            blocks = f_size // self.block_size
            if blocks * self.block_size != f_size:
                blocks += 1
            occupied_blocks += blocks
        if occupied_blocks > self._max_file_blocks or entry is None:
            return -1, None
        return 0, entry


    def read_file(self, file_name, file_entry=None):
        if file_entry is None:
            file_entry = self._get_file_entry(file_name)
            if file_entry is None:
                return None
        data = bytearray()
        disk_idx, block_idx = file_entry['file_disk'], file_entry['file_block']
        has_next = True
        while has_next:
            block = self._read_block(disk_idx, block_idx)
            size = self._block_get_size(block)
            if size == 0:
                break
            data.extend(self._block_get_data(block, size))
            next_disk = self._block_get_next_disk(block)
            next_block = self._block_get_next_block(block)
            has_next = disk_idx != next_disk or block_idx != next_block
            disk_idx, block_idx = next_disk, next_block
        return data


    def add_file(self, file_name, b_data):
        res = self._able_to_add_file(file_name, len(b_data))
        if res != 0:
            return res
        res = self._next_available_block(self._last_table_disk, self._last_table_block)
        if res is None:
            return -1
        disk_idx, block_idx = res
        res = self._add_file_to_table(file_name, len(b_data), disk_idx, block_idx)
        if res != 0:
            return -1
        offset = 0
        while offset < len(b_data):
            if len(b_data) - offset > self.block_data_size:
                tmp = self._next_available_block(disk_idx, block_idx)
                if tmp is None:
                    self.del_file(file_name)
                    return -1
                next_disk, next_block = tmp
                block = bytearray()
                block.extend(self.block_data_size.to_bytes(4, 'little'))
                block.extend(next_disk.to_bytes(4, 'little'))
                block.extend(next_block.to_bytes(4, 'little'))
                block.extend(b_data[offset:offset+self.block_data_size])
                self._write_block(block, disk_idx, block_idx)
                self._reset_pq(block_idx)
                disk_idx, block_idx = next_disk, next_block
                offset += self.block_data_size
            else:
                block = bytearray()
                block.extend((len(b_data) - offset).to_bytes(4, 'little'))
                block.extend(disk_idx.to_bytes(4, 'little'))
                block.extend(block_idx.to_bytes(4, 'little'))
                block.extend(b_data[offset:])
                block.extend(b'\x00' * (self.block_data_size - len(b_data) + offset))
                self._write_block(block, disk_idx, block_idx)
                self._reset_pq(block_idx)
                offset = len(b_data)
        return 0


    def del_file(self, file_name):
        file_entry = self._get_file_entry(file_name)
        if file_entry is None:
            return
        disk_idx, block_idx = file_entry['file_disk'], file_entry['file_block']
        self._del_file_from_table(file_entry)
        while True:
            block = self._read_block(disk_idx, block_idx)
            size = self._block_get_size(block)
            next_disk = self._block_get_next_disk(block)
            next_block = self._block_get_next_block(block)
            data = bytearray(b'\x00' * self.block_size)
            self._write_block(data, disk_idx, block_idx)
            self._reset_pq(block_idx)
            if size == 0:
                break
            if disk_idx == next_disk and block_idx == next_block:
                break
            disk_idx, block_idx = next_disk, next_block


    def modify_file(self, file_name, begin, end, b_data):
        res = self._able_to_modify_file(file_name, begin, end, len(b_data))
        if res[0] != 0 or res[1] is None:
            return res[0]
        file_entry = res[1]
        file_size = file_entry['file_size']
        # change the file size
        if len(b_data) != end - begin:
            f_data = self.read_file(file_name, file_entry)
            new_data = bytearray()
            new_data.extend(f_data[0:begin])
            new_data.extend(b_data)
            new_data.extend(f_data[end:file_size])
            self.del_file(file_name)
            return self.add_file(file_name, new_data)
        # keep the same size
        if begin == end:
            return 0
        offset = 0
        disk_idx, block_idx = file_entry['file_disk'], file_entry['file_block']
        while offset <= end:
            block = self._read_block(disk_idx, block_idx)
            if offset + self.block_data_size <= begin:
                offset += self.block_data_size
                disk_idx = self._block_get_next_disk(block)
                block_idx = self._block_get_next_block(block)
                continue
            block_start = self.block_head_size + max(begin - offset, 0)
            data_start = max(offset - begin, 0)
            data_size = min(end - offset, self.block_data_size) - max(begin - offset, 0)
            block[block_start:block_start+data_size] = b_data[data_start:data_start+data_size]
            self._write_block(block, disk_idx, block_idx)
            self._reset_pq(block_idx)
            disk_idx = self._block_get_next_disk(block)
            block_idx = self._block_get_next_block(block)
            offset += self.block_data_size


    def list_files(self):
        d, b = -1, 0
        entries = []
        while d != self._last_table_disk or b != self._last_table_block :
            d += 1
            if d == self.disk_num:
                d = 0
                b += 1
            if self._get_p_disk(b) == d or self._get_q_disk(b) == d:
                continue
            block = self._read_block(d, b)
            offset = 0
            while offset < self.block_size:
                entry = self._entry_byte_to_dict(block[offset:offset+self._table_entry_size], d, b, offset)
                if entry['file_name'] is not None:
                    entries.append(entry)
                offset += self._table_entry_size
        return entries


    def clear_disk(self, disk_idx):
        self.disk_manager.clear_disk(disk_idx)


    def check_disk(self, disk_idx):
        return self.disk_manager.check_disk(disk_idx)


    def recover_failed_disks(self, d0, d1=None):
        self.disk_manager.clear_disk(d0)
        if d1 is not None:
            self.disk_manager.clear_disk(d1)
        for b in range(self.block_num):
            p_idx = self._get_p_disk(b)
            q_idx = self._get_q_disk(b)
            blocks = [None for _ in range(self.disk_num - 2)]
            for d in range(self.disk_num):
                if d == d0 or d == d1:
                    continue
                blocks[d] = self._read_block(d, b)
            d0_block, d1_block = bytearray(), bytearray()
            for i in range(self.block_size):
                tmp = []
                for d in range(self.disk_num):
                    if blocks[i] is None:
                        tmp.append(None)
                    else:
                        tmp.append(blocks[d][i])
                d0_byte, d1_byte = b'\x00', b'\x00' # TODO calculate
                d0_block.extend(d0_byte)
                d1_block.extend(d1_byte)
            self._write_block(d0_block, d0, b)
            if d1 is not None:
                self._write_block(d1_block, d1, b)


    def check_corrupt(self, block_idx):
        data_blocks = []
        pq_blocks = [None, None]
        p_idx = self._get_p_disk(block_idx)
        q_idx = self._get_q_disk(block_idx)
        for d in range(self.disk_num):
            if d == p_idx:
                pq_blocks[0] = self._read_block(d, block_idx)
            elif d == q_idx:
                pq_blocks[1] = self._read_block(d, block_idx)
            else:
                data_blocks.append(self.recover_failed_disks(d, block_idx))
        data_blocks.extend(pq_blocks)
        corrupt_blocks = set()
        for i in range(self.block_size):
            tmp = [data_blocks[d][i] for d in range(self.disk_num)]
            corrupt = 0 # TODO, calculate
            corrupt_blocks.add(corrupt)
        return corrupt_blocks


    def recover_corruption(self, corrupt):
        pass # TODO


if __name__ == '__main__':
    disk_size = 20 * 1024 * 1024
    block_size = 64 * 1024
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

    file_manager = FileManager(disk_size, block_size, max_file_num, disks)
    for i in range(len(disks)):
        file_manager.clear_disk(i)

    import random, os, shutil
    random.seed(0)
    shutil.make_archive('./test', 'zip', './test/')
    test_files = [os.path.join('./test/', x) for x in os.listdir('./test')]
    exe_steps = 300
    os_files = set()
    out_files = set(test_files)
    for i in range(exe_steps):
        op = random.random()

        # add_file
        if op < 0.2:
            if len(out_files) == 0:
                add_file = list(os_files)[random.randint(0, len(os_files) - 1)]
            else:
                if random.random() < 0.8 or len(os_files) == 0:
                    add_file = list(out_files)[random.randint(0, len(out_files) - 1)]
                else:
                    add_file = list(os_files)[random.randint(0, len(os_files) - 1)]
            os_files.add(add_file)
            if add_file in out_files:
                out_files.remove(add_file)
            print(i, 'add', add_file)
            with open(add_file, 'rb') as fread:
                d0 = fread.read()
            file_manager.add_file(add_file, d0)
            d1 = file_manager.read_file(add_file)
            ls = set([x['file_name'] for x in file_manager.list_files()])
            if os_files != ls or d0 != d1:
                print('error!')
                print('--- os_files ---')
                print(os_files)
                print('--- ls ---')
                print(ls)
                if d0 != d1:
                    print('--- d0 != d1 ---')
                break

        # delete file
        elif op < 0.4:
            if len(os_files) == 0:
                del_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            else:
                if random.random() < 0.8 or len(out_files) == 0:
                    del_file = list(os_files)[random.randint(0, len(os_files) - 1)]
                else:
                    del_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            if del_file in os_files:
                os_files.remove(del_file)
            out_files.add(del_file)
            print(i, 'del', del_file)
            file_manager.del_file(del_file)
            ls = set([x['file_name'] for x in file_manager.list_files()])
            if os_files != ls:
                print('error!')
                print('--- os_files ---')
                print(os_files)
                print('--- ls ---')
                print(ls)
                break

        # read file
        elif op < 0.6:
            if len(os_files) == 0:
                read_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            else:
                if random.random() < 0.8 or len(out_files) == 0:
                    read_file = list(os_files)[random.randint(0, len(os_files) - 1)]
                else:
                    read_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            print(i, 'read', read_file)
            if read_file in os_files:
                with open(read_file, 'rb') as fread:
                    d0 = fread.read()
            else:
                d0 = None
            d1 = file_manager.read_file(read_file)
            if d0 != d1:
                print('error!')
                break

        # modify file
        elif op < 1:
            if len(os_files) == 0:
                modify_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            else:
                if random.random() < 0.8 or len(out_files) == 0:
                    modify_file = list(os_files)[random.randint(0, len(os_files) - 1)]
                else:
                    modify_file = list(out_files)[random.randint(0, len(out_files) - 1)]
            print(i, 'modify', modify_file)
            if modify_file not in os_files:
                begin, end = 10, 20
                new_data = bytearray(os.urandom(10))
                d1 = None
            else:
                with open(modify_file, 'rb') as fm:
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
                d1 = bytearray()
                d1.extend(d0[0:begin])
                d1.extend(new_data)
                d1.extend(d0[end:])
                with open(modify_file, 'wb') as fm:
                    fm.write(d1)
            file_manager.modify_file(modify_file, begin, end, new_data)
            d2 = file_manager.read_file(modify_file)
            if d1 != d2:
                print('error!')
                print('--- os files ---')
                print(os_files)
                print('--- ls ---')
                print(file_manager.list_files())
                print('--- begin, end, file_len, new_len ---')
                print(begin, end, -1 if d1 is None else len(d1), len(new_data))
                break

    shutil.rmtree('./test/')
    shutil.unpack_archive('./test.zip', './test/')
    os.remove('./test.zip')