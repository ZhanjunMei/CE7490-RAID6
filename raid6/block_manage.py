class BlockManager:

    """
    block_format:
    [0-3]: size_in_this_block
    [4-7]: next_disk_idx
    [8-12]: next_block_idx
    [...]: data
    """

    def __init__(self,
                 disk_num,
                 block_size,
                 ):
        self.disk_num = disk_num
        self.block_size = block_size
        self.head_size = 12
        self.data_size = self.block_size - self.head_size
        self.first_available_disk = 0
        self.first_available_block = 0


    def _get_p_idx(self, block_idx):
        return (block_idx + self.disk_num - 2) % self.disk_num


    def _get_q_idx(self, block_idx):
        return (block_idx + self.disk_num - 1) % self.disk_num


    def _get_size(self, b_block):
        return int.from_bytes(b_block[0:4], 'little')


    def _get_next_disk(self, b_block):
        return int.from_bytes(b_block[4:8], 'little')


    def _get_next_block(self, b_block):
        return int.from_bytes(b_block[8:12], 'little')


    def _next_available_block(self, this_disk, this_block):
        return 1, 2  # TODO, return (disk_idx, block_idx)
        pass

    def _get_file_size(self, filename):
        return 0 # TODO, check file table

    def _first_data_block(self, filename):
        return 1, 2 # TODO, return (disk_idx, block_idx)
        pass


    def _read_block(self, disk_idx, block_idx):
        return []
        pass


    def _write_block(self, block, disk_idx, block_idx):
        pass


    def read_file(self, filename):
        data = bytearray()
        disk_idx, block_idx = self._first_data_block(filename)
        while True:
            block = self._read_block(disk_idx, block_idx)
            size = self._get_size(block)
            if size == 0:
                break
            data.extend(block[self.head_size:self.head_size + size])
            if size <= self.data_size:
                break
            disk_idx = self._get_next_disk(block)
            block_idx = self._get_next_block(block)
        return data


    def add_file(self, filename, b_data):
        offset = 0
        disk_idx, block_idx = self._next_available_block(
            self.first_available_disk, self.first_available_block)
        # TODO rewrite file table
        while offset < len(b_data):
            if len(b_data) - offset > self.data_size:
                next_disk, next_block = self._next_available_block(disk_idx, block_idx)
                block = bytearray()
                block.extend(self.data_size.to_bytes(4, 'little'))
                block.extend(next_disk.to_bytes(4, 'little'))
                block.extend(next_block.to_bytes(4, 'little'))
                block.extend(b_data[offset:self.data_size])
                self._write_block(block, disk_idx, block_idx)
                # TODO reset p, q
                disk_idx, block_idx = next_disk, next_block
                offset += self.data_size
            else:
                block = bytearray()
                block.extend((len(b_data) - offset).to_bytes(4, 'little'))
                block.extend(disk_idx.to_bytes(4, 'little'))
                block.extend(block_idx.to_bytes(4, 'little'))
                block.extend(b_data[offset:])
                block.extend(b'\x00' * (self.data_size - len(b_data) + offset))
                self._write_block(block, disk_idx, block_idx)
                # TODO reset p, q
                offset = len(b_data)


    def del_file(self, filename):
        disk_idx, block_idx = self._first_data_block(filename)
        # TODO rewrite file table
        while True:
            block = self._read_block(disk_idx, block_idx)
            size = self._get_size(block)
            next_disk = self._get_next_disk(block)
            next_block = self._get_next_block(block)
            data = bytearray(b'\x00' * self.block_size)
            self._write_block(data, disk_idx, block_idx)
            # TODO reset p, q
            if size > self.data_size:
                disk_idx, block_idx = next_disk, next_block


    def modify_file(self, filename, offset, b_data):
        file_size = self._get_file_size(filename)
        if offset + b_data >= file_size:
            self.del_file(filename)
            self.add_file(filename, b_data)
            return
        current_offset = 0
        disk_idx, block_idx = self._first_data_block(filename)
        while True:
            block = self._read_block(disk_idx, block_idx)
            if current_offset + self.data_size < offset:
                pass # TODO


    def _cal_block_p(self, block_idx):
        blocks = []
        for i in range(self.disk_num):
            if i != self._get_p_idx(block_idx) and i != self._get_q_idx(block_idx):
                blocks.append(self._read_block(i, block_idx))
        block_p = []
        for i in range(self.block_size):
            tmp = [blocks[i][j] for j in range(self.disk_num)]
            p = 0x12 # TODO p = cal_p(tmp)
            block_p.append(p)
        return block_p


    def _cal_block_q(self, block_idx):
        blocks = []
        for i in range(self.disk_num):
            if i != self._get_p_idx(block_idx) and i != self._get_q_idx(block_idx):
                blocks.append(self._read_block(i, block_idx))
        block_q = []
        for i in range(self.block_size):
            tmp = [blocks[i][j] for j in range(self.disk_num)]
            q = 0x12  # TODO q = cal_q(tmp)
            block_q.append(q)
        return block_q
