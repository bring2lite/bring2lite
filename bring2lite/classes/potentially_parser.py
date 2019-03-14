from .parser import Parser
import logging
from struct import *


class PotentiallyParser(Parser):

    def __init__(self):
        self.logger = logging.getLogger("parser.PotentiallyParser")
        #print("#############################################################")

    def parse_page(self, page, filename="", is_first_page=False, is_wal=False, is_trunk_page=False):
        # the schema page is on the first page but these page contain the sqlite header in it
        self.is_wal = is_wal
        self.is_first_page = is_first_page
        self.is_trunk_page = is_trunk_page
        self.filename = filename
        if page:
            self.page_size = len(page)
        else:
            return []

        if self.is_first_page:
            schema_offset = 100
        else:
            schema_offset = 0

        page_header = page[schema_offset + 0:schema_offset + 8]
        pageheader = unpack('>bhhHb', page_header)
        page_type = pageheader[0]
        start_of_first_free_block = pageheader[1]

        self.start_of_cell_content_area = pageheader[3]

        unalloc_content = None
        if self.is_trunk_page:
            unalloc_content = self._extract_trunk_page_content(page)
        else:
            unalloc_content = self._extract_unalloc_content(page, schema_offset)
            if not unalloc_content:
                return []


        self.result = []
        cell_offset = 0
        stop = False
        while not stop:
            try:
                """the 9 because this is the maximum leangth a varint can have"""
                payload_length, index1 = self.single_varint(unalloc_content[cell_offset: cell_offset + 9])
                if payload_length == 0:
                    stop = True
                    continue
                id, index2 = self.single_varint(unalloc_content[cell_offset + index1: cell_offset + index1 + 9])
                current_index = index1 + index2

                header_length, index_after_header = self.single_varint(
                    unalloc_content[cell_offset + current_index: cell_offset + current_index + 9])
                current_index += index_after_header


                serial_types, index3 = self.multi_varint(unalloc_content[
                                                        cell_offset + current_index: cell_offset + current_index + header_length - index_after_header])
                current_index += index3

            except (ValueError, TypeError):
                return []
            """this calculation can be found on the officiel SQLite docu"""
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
                payload_length = var_K
                if not self.is_wal:
                    overflow_content = self._extract_overflow_pages(unalloc_content, cell_offset + current_index + payload_length - header_length, var_P - var_K)
            elif (var_P > var_X) and (var_K > var_X):
                payload_length = var_M
                if not self.is_wal:
                    overflow_content = self._extract_overflow_pages(unalloc_content, cell_offset + current_index + payload_length - header_length, var_P - var_M)

            content_length = payload_length - header_length
            try:
                tempresult = self._typeHelper(serial_types, unalloc_content[cell_offset + current_index: cell_offset + current_index + content_length] + overflow_content)
                self.result.append(tempresult)
            except (ValueError, error):
                stop = True
                continue

            if len(unalloc_content) >= self.page_size:
                stop = True
                continue
            try:
                tester = unpack('>H', unalloc_content[cell_offset + current_index + content_length: cell_offset + current_index + content_length + 2])[0]
            except error:
                return self.result

            cell_offset += current_index + content_length

            if cell_offset == len(unalloc_content):
                stop = True

        return self.result

    """Searching a page to find the unallocated space and extract it"""
    def _extract_unalloc_content(self, page, schema_offset):
        result = None
        page_header = unpack('>bhhHb', page[schema_offset: schema_offset + 8])

        if (page_header[0] != 13):
            return None
        num_of_cells_in_frame = page_header[2]
        start_of_cell_content_area = page_header[3]
        size_of_unalloc_space = None

        size_of_unalloc_space = start_of_cell_content_area - (schema_offset + 8 + (num_of_cells_in_frame*2))
        unalloc_area = page[(schema_offset + 8 + (num_of_cells_in_frame*2)): (schema_offset + 8 + (num_of_cells_in_frame*2)) + size_of_unalloc_space]
        pointer = 0
        cell_pointer = 0
        try:
            cell_pointer = unpack('>H', unalloc_area[pointer: pointer+2])[0]
        except error:
            pass
        """loop over cell pointer array"""
        while cell_pointer:
            pointer += 2
            try:
                cell_pointer = unpack('>H', unalloc_area[pointer: pointer + 2])[0]
            except error:
                cell_pointer = None
                pointer -= 2

        result = unalloc_area[pointer:]

        start_of_content = 0
        offset_tester = 0
        try:
            offset_tester = unpack('>B', result[start_of_content: start_of_content + 1])[0]
        except error:
            pass

        while not offset_tester:
            start_of_content = start_of_content+1
            try:
                offset_tester = unpack('>B', result[start_of_content: start_of_content + 1])[0]
            except error:
                return None
        """return the raw found content"""
        result = result[start_of_content:]
        return result

    """Trunk pages can also contain deleted cells"""
    def _extract_trunk_page_content(self, page):
        point_counter = unpack('>I', page[4:8])[0]
        end_of_cell_pointer_array = 8 + (point_counter * 4)

        offset_tester = unpack('>H', page[end_of_cell_pointer_array: end_of_cell_pointer_array + 2])[0]
        while offset_tester:
            end_of_cell_pointer_array += 2
            try:
                offset_tester = unpack('>H', page[end_of_cell_pointer_array: end_of_cell_pointer_array + 2])[0]
            except error:
                break

        tmp_result = page[end_of_cell_pointer_array: self.page_size]

        start_of_content = 0
        offset_tester = unpack('>B', tmp_result[start_of_content: start_of_content + 1])[0]
        while not offset_tester and (start_of_content != len(tmp_result)):
            start_of_content += 1
            try:
                offset_tester = unpack('>B', tmp_result[start_of_content: start_of_content + 1])[0]
            except error:
                break
        result = tmp_result[start_of_content:self.page_size]

        return result
