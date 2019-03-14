import abc
from struct import *
import binascii

import ntpath

class Parser:
    __metaclass__ = abc.ABCMeta

    WAL_HEADER_SIZE = 32
    FRAME_HEADER_SIZE = 24

    def __init__(self):
        self.result = []
        self.filename = None
        self.page_size = 0
        self.logger = logging.getLogger("parser.parser")

    @abc.abstractmethod
    def parse(self, fname, outname):
        raise NotImplementedError

    @abc.abstractmethod
    def _parse_header(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _parse_body(self):
        raise NotImplementedError

    @abc.abstractmethod
    def _parse_page(self, page, is_first_page=False, is_wal=False):
        #the schema page is on the first page but these page contain the sqlite header in it
        self.is_wal = is_wal
        if is_first_page:
            schema_offset = 100
        else:
            schema_offset = 0

        page_header = unpack('>bhhHb', page[schema_offset+0:schema_offset+8])
        page_type = page_header[0]
        start_of_first_free_block = page_header[1]
        #
        #if zero then show the cell pointer array
        #
        num_of_cells_in_frame = page_header[2]
        start_of_cell_content_area = page_header[3]

        if not page_type:
            return None #[page]

        if page_type != 13:

            return page_type

        return self._extract_cells(page, num_of_cells_in_frame, schema_offset)

    def _extract_cells(self, page, num_of_cells_in_frame, schema_offset):
        res = []
        """loop over every pointer to a cell"""
        for x in range(num_of_cells_in_frame):
            # an array of pointer are stored in the first few bytes
            start_of_cell_parsing = schema_offset + 8 + (x * 2)
            # grap the cell_offset of the cell X
            cell_offset = unpack('>h', page[start_of_cell_parsing: start_of_cell_parsing + 2])[0]
            # Calculating the varint of the payload and the rowID
            # keep the index to calculate the header of the cell
            payload_length, index1 = self.single_varint(page[cell_offset: cell_offset + 9])
            id, index2 = self.single_varint(page[cell_offset + index1: cell_offset + index1 + 9])
            current_index = index1 + index2
            header_length, index_after_header = self.single_varint(
                page[cell_offset + current_index: cell_offset + current_index + 9])

            current_index += index_after_header

            cell_types, index_c = self.multi_varint(
                page[cell_offset + current_index: cell_offset + current_index + header_length - index_after_header])
            current_index += index_c

            """
            calculating overflow pages
            """
            var_U = int(len(page))
            var_X = int(var_U - 35)
            var_M = int(((var_U - 12) * 32 / 255) - 23)
            var_P = int(payload_length)
            var_K = int((var_M + ((var_P - var_M) % (var_U - 4))))
            """
            extracting overflow pages
            """
            overflow_content = b""
            if (var_P > var_X) and (var_K <= var_X):
                print("extract overflow")
                payload_length = var_K
                if not self.is_wal:
                    overflow_content = self._extract_overflow_pages(page, cell_offset + current_index + payload_length - header_length, var_P - var_K)
            elif (var_P > var_X) and (var_K > var_X):
                payload_length = var_M
                print("extract overflow")
                if not self.is_wal:

                    overflow_content = self._extract_overflow_pages(page, cell_offset + current_index + payload_length - header_length, var_P - var_M)


            content_length = payload_length - header_length
            current_page_cell_content = page[cell_offset + current_index: cell_offset + current_index + content_length] + overflow_content

            tempresult = self._typeHelper(cell_types, current_page_cell_content)

            res.append(tempresult)

        self.logger.debug("end parsing cells")
        return res

    def _extract_overflow_pages(self, page, first_overflow_page_offset, size_to_extract):
        result = b''
        try:
            overflow_page_number = unpack('>I', page[first_overflow_page_offset: first_overflow_page_offset + 4])[0]
        except error:
            return result

        with open(self.filename, "rb") as f:
            f.seek(self.page_size * (overflow_page_number))
            p = f.read(self.page_size)
            if not p:
                return b""
            next_page = unpack('>I', p[:4])[0]

            if next_page:
                result += (p[4:self.page_size - 4])
            else:
                result = result + p[4: 4 + size_to_extract]
                return result

            while next_page:
                if not self.is_wal:
                    f.seek(self.page_size * (next_page))
                else:
                    f.seek(next_page * (self.page_size + self.FRAME_HEADER_SIZE) + self.WAL_HEADER_SIZE)
                p = f.read(self.page_size)
                try:
                    next_page = unpack('>I', p[:4])[0]
                except error:
                    break
                if next_page:
                    result += (p[4: self.page_size - 4])
                else:
                    result += (p[4:4 + size_to_extract])
                    return result

        return result

    def single_varint(self, data, index=0):
        varint = 0
        for i in range(0, 9):
            index += 1
            if ord(data[i:i + 1]) < 128:
                break
        tmp = ""
        for i in range(0, index):
            tmp += format((ord(data[i:i+1])), '#010b')[3:]

        varint = int(tmp, 2)
        return varint, index

    """This function inspired from learning python for forensics(ISBN:978-1-78328-523-5)"""
    def multi_varint(self, data):
        varints = []
        index = 0

        while len(data) != 0:
            varint, index_a = self.single_varint(data)
            varints.append(varint)
            index += index_a

            data = data[index_a:]

        return varints, index

    """This function inspired from learning python for forensics(ISBN:978-1-78328-523-5)"""
    def _typeHelper(self, types, data):
        cell_data = []
        index = 0
        for t in types:
            if t == 0:
                cell_data.append(['NULL', 'NULL'])
            elif t == 1:
                d = ["8bit", unpack('>b', data[index:index + 1])[0]]
                cell_data.append(d)
                index += 1
            elif t == 2:
                d = ["16bit", unpack('>h', data[index:index + 2])[0]]
                cell_data.append(d)
                index += 2
            elif t == 3:
                d = ["24bit", int(binascii.hexlify(data[index:index + 3]), 16)]
                cell_data.append(d)
                index += 3
            elif t == 4:
                d = ["32bit", unpack('>i', data[index:index + 4])[0]]
                cell_data.append(d)
                index += 4
            elif t == 5:
                d = ["48bit", int(binascii.hexlify(data[index:index + 6]), 16)]
                cell_data.append(d)
                index += 6
            elif t == 6:
                d = ["64bit", unpack('>q', data[index:index + 8])[0]]
                cell_data.append(d)
                index += 8
            elif t == 7:
                d = ["64bitf", unpack('>d', data[index:index + 8])[0]]
                cell_data.append(d)
                index += 8
            elif t == 8:
                d = ["0", 0]
                cell_data.append(d)
            elif t == 9:
                d = ["1", 1]
                cell_data.append(d)
            elif t > 12 and t % 2 == 0:
                b_length = (t - 12) / 2
                d = ["BLOB", data[index:index + int(b_length)]]
                cell_data.append(d)
                index += int(b_length)
            elif t > 13 and t % 2 == 1:
                s_length = (t - 13) / 2
                d = ["TEXT", data[index:index + int(s_length)]]
                cell_data.append(d)
                index += int(s_length)

        return cell_data

    def _path_leaf(self, path):
        head, tail = ntpath.split(path)
        return tail or ntpath.basename(head)

    """Searching all b-tree leaf pages in the interior page"""
    def collect_pages_from_interior_pages(self, page, is_first_page=False):
        result = []
        if is_first_page:
            schema_offset = 100
        else:
            schema_offset = 0

        page_header = unpack('>bhhHbL', page[schema_offset:schema_offset+12])
        if page_header[0] == 5:
            result.append(page_header[5])
            num_of_cells_in_frame = page_header[2]
            """process the cell-offset array to get the leaf page numbers"""
            for x in range(num_of_cells_in_frame):
                start_of_cell_parsing = schema_offset + 12 + (x * 2)
                cell_offset = unpack('>h', page[start_of_cell_parsing: start_of_cell_parsing + 2])[0]
                cell = unpack('>I', page[cell_offset: cell_offset + 4])
                result.append(cell[0])
        else:
            return None

        return result



