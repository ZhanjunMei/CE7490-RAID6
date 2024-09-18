import time

from .fault_tolerance import failure_fix, corruption_check_fix, compute_PQ
from .disk_manager import DiskManager


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
        self.block_num = int(disk_size // block_size)
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
        # recovery
        self._recovery_time = None

    def _init_file_table(self, max_files):
        max_file_num = (self.disk_num - 2) * (self.disk_size // self.block_size)
        if max_files is not None:
            max_file_num = min(max_files, max_file_num)
        max_entry_num_per_block = int(self.block_size // self._table_entry_size)
        max_table_blocks = int(max_file_num // max_entry_num_per_block)
        if max_file_num % max_entry_num_per_block != 0:
            max_table_blocks += 1
        self._max_file_blocks = (self.disk_num - 2) * (self.disk_size // self.block_size) - max_table_blocks
        res = max_table_blocks % (self.disk_num - 2)
        self._last_table_block = max_table_blocks // (self.disk_num - 2)
        if res == 0:
            self._last_table_disk = self.disk_num - 1
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
            res, block = self._read_block(d, b)
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
            res, block = self._read_block(d, b)
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
            res, block = self._read_block(d, b)
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
        res, block = self._read_block(d, b)
        offset = file_entry['entry_offset']
        block[offset:offset+self._table_entry_size] = bytearray(b'\x00' * self._table_entry_size)
        self._write_block(block, d, b)
        self._reset_pq(b)


    def _disk_real_to_algo(self, idx, p_idx, q_idx):
        if idx == p_idx:
            return self.disk_num - 2
        if idx == q_idx:
            return self.disk_num - 1
        if p_idx == self.disk_num - 1:
            return idx - 1
        if idx > p_idx:
            return idx - 2
        return idx


    def _disk_algo_to_real(self, idx, p_idx, q_idx):
        if idx == self.disk_num - 2:
            return p_idx
        if idx == self.disk_num - 1:
            return q_idx
        if p_idx == self.disk_num - 1:
            return idx + 1
        if idx >= p_idx:
            return idx + 2
        return idx


    def _recover_stripe_blocks_from_failure(self, strip_blocks, failed_disks):
        if len(failed_disks) == 1:
            new_blocks = [bytearray()]
            for i in range(self.block_size):
                arr = [strip_blocks[d][i] for d in range(len(strip_blocks))]
                recover = failure_fix(arr, failed_disks)
                new_blocks[0].append(recover[0])
            return new_blocks
        elif len(failed_disks) == 2:
            new_blocks = [bytearray(), bytearray()]
            for i in range(self.block_size):
                arr = [strip_blocks[d][i] for d in range(len(strip_blocks))]
                recover = failure_fix(arr, failed_disks)
                new_blocks[0].append(recover[0])
                new_blocks[1].append(recover[1])
            return new_blocks


    def _recover_stripe_from_failure(self, block_idx):
        p_idx, q_idx = self._get_p_disk(block_idx), self._get_q_disk(block_idx)
        block_stripe, pq_blocks = [], [None, None]
        failed_disks = []
        for d in range(self.disk_num):
            if self.disk_manager.check_block(d, block_idx) != 0:
                failed_disks.append(d)
                if len(failed_disks) > 2:
                    raise Exception('Failure in more than 2 disks of a stripe!')
        if len(failed_disks) == 0:
            return
        algo_disks = []
        for d in failed_disks:
            algo_disks.append(self._disk_real_to_algo(d, p_idx, q_idx))
        if len(algo_disks) == 2 and algo_disks[0] > algo_disks[1]:
            algo_disks[0], algo_disks[1] = algo_disks[1], algo_disks[0]
            failed_disks[0], failed_disks[1] = failed_disks[1], failed_disks[0]
        for d in range(self.disk_num):
            if d not in failed_disks:
                res, block_data = self.disk_manager.read_block(d, block_idx)
            else:
                block_data = bytearray(b'\x00' * self.block_size)
            if d == p_idx:
                pq_blocks[0] = block_data
            elif d == q_idx:
                pq_blocks[1] = block_data
            else:
                block_stripe.append(block_data)
        block_stripe.extend(pq_blocks)
        res = self._recover_stripe_blocks_from_failure(block_stripe, algo_disks)
        self._write_block(res[0], failed_disks[0], block_idx, force=True)
        if len(failed_disks) == 2:
            self._write_block(res[1], failed_disks[1], block_idx, force=True)


    def _recover_from_failure(self, block_idx):
        t0 = time.time()
        failed_disks = []
        for i in range(self.disk_num):
            if self.disk_manager.check_disk(i) != 0:
                failed_disks.append(i)
                if len(failed_disks) > 2:
                    raise Exception('Failure in more than 2 disks!')
        if len(failed_disks) > 0:
            for b in range(self.block_num):
                self._recover_stripe_from_failure(b)
        else:
            self._recover_stripe_from_failure(block_idx)
        t1 = time.time()
        self._recovery_time = t1 - t0


    def _read_block(self, disk_idx, block_idx, no_failure=False):
        res, data = self.disk_manager.read_block(disk_idx, block_idx)
        if res != 0:
            if no_failure:
                raise Exception('Unable to handle failure!')
            self._recover_from_failure(block_idx)
            res, data = self.disk_manager.read_block(disk_idx, block_idx)
            if res != 0:
                raise Exception('Unable to handle failure after recovery!')
        return res, data


    def _write_block(self, block, disk_idx, block_idx, no_failure=False, force=False):
        res = self.disk_manager.write_block(block, disk_idx, block_idx, force=force)
        if res != 0:
            if no_failure:
                raise Exception('Unable to handle failure!')
            self._recover_from_failure(block_idx)
            res = self.disk_manager.write_block(block, disk_idx, block_idx, force=force)
            if res != 0:
                raise Exception('Unable to handle failure after recovery!')
        return res


    def _cal_block_pq(self, block_idx):
        blocks = []
        for i in range(self.disk_num):
            if i != self._get_p_disk(block_idx) and i != self._get_q_disk(block_idx):
                res, block_data = self._read_block(i, block_idx)
                blocks.append(block_data)
        block_p, block_q = bytearray(), bytearray()
        for j in range(self.block_size):
            arr = [blocks[i][j] for i in range(len(blocks))]
            arr.extend([0, 0])
            res = compute_PQ(arr)
            block_p.append(res[0])
            block_q.append(res[1])
        return block_p, block_q


    def _reset_pq(self, block_idx):
        block_p, block_q = self._cal_block_pq(block_idx)
        self._write_block(block_p, self._get_p_disk(block_idx), block_idx)
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
        if file_entry['file_size'] == 0:
            return data
        disk_idx, block_idx = file_entry['file_disk'], file_entry['file_block']
        has_next = True
        while has_next:
            res, block = self._read_block(disk_idx, block_idx)
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
        if file_entry['file_size'] == 0:
            return
        while True:
            res, block = self._read_block(disk_idx, block_idx)
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
            res, block = self._read_block(disk_idx, block_idx)
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
            res, block = self._read_block(d, b)
            offset = 0
            while offset < self.block_size:
                entry = self._entry_byte_to_dict(block[offset:offset+self._table_entry_size], d, b, offset)
                if entry['file_name'] is not None:
                    entries.append(entry)
                offset += self._table_entry_size
        return entries


    def reset_disk(self, disk_idx):
        return self.disk_manager.reset_disk(disk_idx)


    def get_recovery_time(self):
        ret = self._recovery_time
        if ret is not None:
            self._recovery_time = None
        return ret


    def check_and_recover_corruption(self, block_idx):
        block_stripe, pq_blocks = [], [None, None]
        p_idx, q_idx = self._get_p_disk(block_idx), self._get_q_disk(block_idx)
        for d in range(self.disk_num):
            res, block_data = self._read_block(d, block_idx, no_failure=True)
            if d == p_idx:
                pq_blocks[0] = block_data
            elif d == q_idx:
                pq_blocks[1] = block_data
            else:
                block_stripe.append(block_data)
        block_stripe.extend(pq_blocks)
        algo_disk, recover_entries, recover_block = -1, [], bytearray()
        for i in range(self.block_size):
            arr = [block_stripe[d][i] for d in range(self.disk_num)]
            res_disk, res_data = corruption_check_fix(arr)
            if res_disk < 0:
                continue
            if algo_disk == -1:
                algo_disk = res_disk
            elif algo_disk != res_disk:
                raise Exception('More than 1 block corrupted in a stripe!')
            recover_entries.append((i, res_data))
        if algo_disk == -1:
            return
        disk_idx = self._disk_algo_to_real(algo_disk, p_idx, q_idx)
        res, corrupted_data = self._read_block(disk_idx, block_idx, no_failure=True)
        t = 0
        for i in range(self.block_size):
            if t >= len(recover_entries) or i < recover_entries[t][0]:
                recover_block.append(corrupted_data[i])
            else:
                recover_block.append(recover_entries[t][1])
                t += 1
        self._write_block(recover_block, disk_idx, block_idx, no_failure=True)


    def fail_disk(self, disk_idx):
        return self.disk_manager.fail_disk(disk_idx)


    def corrupt_block(self, disk_idx, block_idx):
        return self.disk_manager.corrupt_block(disk_idx, block_idx)


if __name__ == '__main__':
    pass