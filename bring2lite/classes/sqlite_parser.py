from builtins import print

from .parser import Parser
from .report_generator import ReportGenerator
from .potentially_parser import PotentiallyParser
import logging
import os
import binascii
from struct import *
from tqdm import tqdm
import sqlparse
import re

d = {}
class SQLiteParser(Parser):
    MAGIC_NUMBER = binascii.unhexlify(b'53514c69746520666f726d6174203300')
    HEADER_SIZE = 100

    def __init__(self):
        self.logger = logging.getLogger("parser.SQLiteParser")
        self.filename = ''
        self.file_size = ''
        self.rgen = ReportGenerator()

    def parse(self, filename, outname, format):
        self.filename = filename
        self.outname = outname
        # CSV = 0 | XML = 1 | JSON = 2
        self.forma = format
        if not os.path.isfile(self.filename):
            raise IOError
        self.file_size = os.stat(self.filename).st_size

        self._parse_header()
        self._parse_schema()
        self._parse_body()
        return d
    """extracting essential information from the sqlite main file header"""
    def _parse_header(self):
        with open(self.filename, "rb") as f:
            self.logger.debug("read header")
            sqlite_string = f.read(16)
            if not sqlite_string == self.MAGIC_NUMBER:
                return

            f.seek(16)
            header = f.read(self.HEADER_SIZE - 44)
        self.sqlite_header = unpack('>HBBBBBBIIIIIIIIIIII', header)
        self.page_size = self.sqlite_header[0]
        if self.page_size == 1:
            self.page_size = 65536

        self.file_format_write_version = self.sqlite_header[1]
        self.filge_format_read_version = self.sqlite_header[2]
        self.reserved_unused_space_at_end_of_each_page = self.sqlite_header[3]
        self.max_embedded_payload_fraction = self.sqlite_header[4]
        self.min_embedded_payload_fraction = self.sqlite_header[5]
        self.leaf_payload_fraction = self.sqlite_header[6]
        self.file_change_counter = self.sqlite_header[7]
        self.size_of_database_file_in_pages = self.sqlite_header[8]
        self.page_number_of_first_freelist_trunk_page = self.sqlite_header[9]
        self.total_number_of_freelist_pages = self.sqlite_header[10]
        self.schema_cookie = self.sqlite_header[11]
        self.schema_format_number = self.sqlite_header[12]
        self.default_page_cache_size = self.sqlite_header[13]
        self.page_number_of_the_largest_root_b_tree_page = self.sqlite_header[14]
        self.text_encoding = self.sqlite_header[15]
        self.user_version = self.sqlite_header[16]
        self.vacuum_mode = self.sqlite_header[17]
        self.application_id = self.sqlite_header[18]
        self.logger.debug("end parsing main file header")
    """parsing the shema written on page one in the SQLite master table"""
    def _parse_schema(self):
        with open(self.filename, "rb") as f:
            f.seek(0)
            first_page = f.read(self.page_size)
            result = super(SQLiteParser, self)._parse_page(first_page, True)
            """check if the first page is an interior page"""
            if result == 5:
                result = []
                schema_leaf_pages = super(SQLiteParser, self).collect_pages_from_interior_pages(first_page, True)
                for page in schema_leaf_pages:
                    f.seek((page-1) * self.page_size)
                    tmp = super(SQLiteParser, self)._parse_page(f.read(self.page_size))
                    for i in tmp:
                        result.append(i)
            """if no result by parsing the page regular, the potential parser tries to find traces"""
            if not result:
                p_parser = PotentiallyParser()
                result = p_parser.parse_page(first_page, self.filename, is_first_page=True)
                self.parsed_schemas = self._extract_schemas(result)

            else:
                self.parsed_schemas = self._extract_schemas(result)

            self.schema_related_pages = self._connect_schema_and_pages()
            self.rgen.generate_schema_report(os.path.abspath(self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/schemas/" + "/"),
                                            "sql-schema", self.parsed_schemas, False)
            # self.rgen.generate_schema_report(os.path.abspath(self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/schemas/" + "/"),
            #                                 "sql-schema", self.schema_related_pages, False)
            global d

            d["schema"] = self.parsed_schemas
            d["schema-column"] = self._extract_column_names(result)
            d["schema_related_pages"] = self.schema_related_pages


    """parsing sqlite main file body"""
    def _parse_body(self):
        global d
        d["body"] = {}
        self.current_schema = []
        with open(self.filename, "rb") as binary_file:
            max = self.file_size / self.page_size
            """loop over all pages"""
            for counter in range(1, int(max)):
                for k, v in d["schema"].items():
                    if k == counter+1:
                        self.current_schema = v
                self.current_page = counter + 1
                d["body"][self.current_page] = {}
                binary_file.seek(counter * self.page_size)
                one_page = binary_file.read(self.page_size)
                """parse the page regular"""
                result = super(SQLiteParser, self)._parse_page(one_page)
                if not isinstance(result, int):
                    d["body"][self.current_page]["page"] = result
                    self.rgen.generateReport(os.path.abspath(self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/regular-page-parsing/" + "/"),
                                         str(self.current_page) + "-page", result, schema=self.current_schema)

                """add 1 to the counter because the first page got the number 1 not 0, second got 2 not 1 etc."""
                freeblocks = self._parse_freeblocks(counter + 1, one_page)
                if freeblocks:
                    if len(freeblocks) > 0:
                        d["body"][self.current_page]["freeblocks"] = freeblocks
                        self.rgen.generate_freeblock_report(os.path.abspath(self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/freeblocks/" + "/"),
                                                            str(counter) + "-page-freeblocks", freeblocks)
                """search for unallocated areas or empty pages"""
                p_parser = PotentiallyParser()
                if result is not 2 and result is not 10:
                    result = p_parser.parse_page(one_page, self.filename)
                    if result:
                        d["body"][self.current_page]["unalloc"] = result
                        self.rgen.generateReport(os.path.abspath(
                            self.outname + "/" +
                            super(SQLiteParser, self)._path_leaf(self.filename) + "/unalloc-parsing/" + "/"),
                            str(counter) + "-page", result, schema=self.current_schema)
                    counter += 1

        self._parse_freelists()

        self.logger.debug("end parsing body")

    """parsing freelists entry point is in the sqlite main file header"""
    def _parse_freelists(self):
        global d
        self.freelist_leaf_page_pointer = []
        flisttp = None
        with open(self.filename, "rb") as f:
            if self.page_number_of_first_freelist_trunk_page:
                """extracting the first freelist"""
                f.seek((self.page_number_of_first_freelist_trunk_page - 1) * self.page_size)
                flisttp = f.read(self.page_size)
                self.freelist_trunk_page_header = unpack('>ii', flisttp[:8])
                """is there any additional page"""
                if self.freelist_trunk_page_header[1] > 0:
                    for x in range(self.freelist_trunk_page_header[1]):
                        ptr_array_offset = 8 + (x*4)
                        ptr = unpack('>i', flisttp[ptr_array_offset:ptr_array_offset+4])
                        self.freelist_leaf_page_pointer.append(ptr[0])
        """searching the not allocated area of a freelist with the potential parser"""
        p_parser = PotentiallyParser()
        res = p_parser.parse_page(flisttp, self.filename, is_trunk_page=True)
        if res:
            d["body"]["flist-trunk"] = res
            self.rgen.generateReport(os.path.abspath(
                self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/freelists/freelist_trunk_pages" + "/"),
                                 str(self.page_number_of_first_freelist_trunk_page) + "-page", res, False)

        self._parse_freelist_leaf_pages(self.freelist_leaf_page_pointer)
        self.logger.debug("end parsing freelist")

    def _parse_freelist_leaf_pages(self, ptrs):
        global d
        d["body"]["flis-leaf"] = {}
        for ptr in ptrs:
            with open(self.filename, "rb") as f:
                f.seek((ptr - 1) * self.page_size)
                leaf_page = f.read(self.page_size)

                res = super(SQLiteParser, self)._parse_page(leaf_page)
                """searching deleted content in the freelist leaf page"""
                if not res and isinstance(res, list):
                    p_parser = PotentiallyParser()
                    res = p_parser.parse_page(leaf_page, self.filename)
                d["body"]["flis-leaf"][ptr] = res
                self.rgen.generateReport(os.path.abspath(self.outname + "/" + super(SQLiteParser, self)._path_leaf(self.filename) + "/freelists/" + "/"),
                                         str(ptr) + "-page", res, False)

    def _parse_freeblocks(self, page_number, page):
        HEADER_TYPES = [2, 5, 10, 13]
        page_header = page[0:8]
        pageheader = unpack('>bhhHb', page_header)
        page_type = pageheader[0]
        if not page_type in HEADER_TYPES:
            return
        start_of_first_free_block = pageheader[1]
        num_of_cells_in_frame = pageheader[2]
        start_of_cell_content_area = pageheader[3]

        result = []
        """loop over the whole chain of freeblocks"""
        while start_of_first_free_block:
            size_of_freeblock = unpack(">h", page[(start_of_first_free_block + 2):(start_of_first_free_block + 4)])[0]
            freeblock = page[start_of_first_free_block:(start_of_first_free_block + size_of_freeblock)]
            start_of_first_free_block = unpack(">h", page[start_of_first_free_block: start_of_first_free_block+2])[0]
            schema_for_page_number = ""

            """lookup which schema describe the current page"""
            for k in self.schema_related_pages:
                if page_number in self.schema_related_pages[k]:
                    schema_for_page_number = k

            for pages_for_schema in self.schema_related_pages.values():
                if page_number in pages_for_schema:
                    for i in range(3, 27):
                        res = None
                        try:
                            res = self._extract_cell(i, freeblock, self.parsed_schemas[schema_for_page_number])
                        except TypeError:
                            pass
                            #break
                        if not res:
                            continue
                        else:
                            result.append(res)
        self.logger.debug("end parsing freeblocks")
        return result

    def _extract_cell(self, estimated_varint_length, freeblock, schema):
        """extract the full length of the freeblock out of the freeblock-header"""
        freeblock_length = unpack('>H', freeblock[2:4])[0]
        """all types of int that can be describe the size of the stored value"""
        INT_TYPES = [1, 2, 3, 4, 5, 6]
        """offset if there is anx text or blob in the schema"""
        text_blob_offset = 0
        """stores the types that are stored in the cell"""
        record_types = []
        """counter to store the number of text or blob fields in the sql-schema"""
        values_with_blob_or_text = 0
        """container to store the possible solutions if there are more than one cell into the freeblock"""
        possible_solutions = []

        #TODO: maybe change this solution later, but at the moment this is fine
        if schema[0] != 'INT':
            raise TypeError("first column in the schema is not an integer")

        """Count all columns in the schema that are BLOB or TEXT because they could be longer then other fields"""
        for i in range(1, len(schema)):
            if self._type_is_longer(schema[i]):
                values_with_blob_or_text += 1

        """If there are no BLOB or TEXT, we don't need to vary the size of the schema types"""
        if values_with_blob_or_text == 0:
            try:
                record_types, index = self.multi_varint(freeblock[estimated_varint_length:
                                                                  estimated_varint_length +
                                                                  len(schema)])  # + values_with_blob_or_text
            except (TypeError, ValueError):
                pass
        else:
            """but if there are some, we need to compare the schema with the extracted record types"""
            for i in range(0, values_with_blob_or_text):
                try:
                    record_types, index = self.multi_varint(freeblock[estimated_varint_length:
                                                                      estimated_varint_length +
                                                                      len(schema) + i])  #+ values_with_blob_or_text

                    converted_record_types = self._cast_record_types_to_schema(record_types)
                    if len(converted_record_types) > 0:
                        converted_record_types[0] = "INT"
                    if self._is_schema_and_types_the_same(schema, converted_record_types):
                        text_blob_offset = i
                        break
                except (TypeError, ValueError):
                    pass

        """we have to differentiate if the varint section is exact 3 byte (the minimum length of this section) or 
        else we can extract all informations out of the record types.
        The rest of this part is the calculation of the start and end of the content area of the overwritten cell"""
        if estimated_varint_length == 3 and len(record_types) > 0:
            tmp_result = 0
            for i in INT_TYPES:
                record_types[0] = i
                converted_record_types = self._cast_record_types_to_schema(record_types)
                if self._is_schema_and_types_the_same(schema, converted_record_types):
                    lenght_of_cell_content = self._calculate_length_of_freeblock_record_data(record_types)
                    estimated_freeblock_length = estimated_varint_length + len(schema) + lenght_of_cell_content + \
                                                 text_blob_offset
                    estimated_start_of_content_area = estimated_varint_length + len(schema) + text_blob_offset
                    tmp_cell_data = freeblock[estimated_start_of_content_area: freeblock_length]
                    if estimated_freeblock_length <= freeblock_length:
                        tmp_result = self._typeHelper(record_types, tmp_cell_data)
                        possible_solutions.append(tmp_result)
                    else:
                        return tmp_result

        else:
            tmp_result = 0
            converted_record_types = self._cast_record_types_to_schema(record_types)
            if self._is_schema_and_types_the_same(schema, converted_record_types):
                lenght_of_cell_content = self._calculate_length_of_freeblock_record_data(record_types)
                estimated_freeblock_length = estimated_varint_length + len(schema) + lenght_of_cell_content + \
                                             text_blob_offset
                estimated_start_of_content_area = estimated_varint_length + len(schema) + text_blob_offset
                tmp_cell_data = freeblock[estimated_start_of_content_area: freeblock_length]
                """if we hit the max size of the freeblock, we return the latest result"""
                if estimated_freeblock_length <= freeblock_length:
                    tmp_result = self._typeHelper(record_types, tmp_cell_data)
                    possible_solutions.append(tmp_result)
                else:
                    return tmp_result

        return possible_solutions

    def _type_is_longer(self, t):
        if t == 'TEXT' or t == 'BLOB':
            return True
        return False

    """sums up the size of a given list of record types"""
    def _calculate_length_of_freeblock_record_data(self, record_types):
        result = 0
        for i in record_types:
            result += self._calculate_size(i)
        return result

    """converts the record types to the equivalent size"""
    def _calculate_size(self, val):
        if val == 1:
            result = 1
        elif val == 2:
            result = 2
        elif val == 3:
            result = 3
        elif val == 4:
            result = 4
        elif val == 5:
            result = 6
        elif val == 6:
            result = 8
        elif val == 7:
            result = 8
        elif val > 12 and val % 2 == 0:
            result = (val - 12) / 2
        elif val > 13 and val % 2 == 1:
            result = (val - 13) / 2
        else:
            result = 0

        return result

    """converting numbers of integertypes to strings"""
    def _cast_record_types_to_schema(self, types):
        result = []
        for s in types:
            if s == 1:
                result.append('INT')
            elif s == 2:
                result.append('INT')
            elif s == 3:
                result.append('INT')
            elif s == 4:
                result.append('INT')
            elif s == 5:
                result.append('INT')
            elif s == 6:
                result.append('INT')
            elif s == 7:
                result.append('REAL')
            elif s > 12 and s % 2 == 0:
                result.append('BLOB')
            elif s > 13 and s % 2 == 1:
                result.append('TEXT')
            else:
                pass

        return result

    """compare if the length and the items itself are equivalent to each other"""
    def _is_schema_and_types_the_same(self, schema, types, varint_offset=4):
        result = True
        start = 0

        if varint_offset == 3:
            start = 1
            print("offset to small don't start at offset 0")
        if len(types) != len(schema):
            return False

        for i in range(start, len(schema)):
            if schema[i] != types[i]:
                return False

        return result

    """extract the one sql-statement out of the processed first page of an SQLite database
    returns the values which describe the columns of the schema"""
    def _extract_schemas(self, schema):
        result = {}
        for row in schema:
            if row and (row[0][1]).decode("utf-8") == "table":
                sql = (row[4][1]).decode("utf-8")
                if "VIRTUAL" in sql:
                    continue
                sql_create_string = sqlparse.parse(sql)
                lines_of_sql = str(sql_create_string[0].tokens[-1]).split(',')
                #print(lines_of_sql)
                for i in range(len(lines_of_sql)):
                #for i in range(len(lines_of_sql)):
                    #print(lines_of_sql[i])
                    cleaned_symbols = self._erase_symbols_from_sql_statement(lines_of_sql[i])

                    #if not cleaned_symbols:
                    #    lines_of_sql.remove(lines_of_sql[i])
                    #else:
                    lines_of_sql[i] = cleaned_symbols
                result[row[3][1]] = lines_of_sql
        return result

    def _extract_column_names(self, schema):
        result = {}
        for row in schema:
            if row and (row[0][1]).decode("utf-8") == "table":
                sql = (row[4][1]).decode("utf-8")
                if "VIRTUAL" in sql:
                    continue
                sql_create_string = sqlparse.parse(sql)
                lines_of_sql = str(sql_create_string[0].tokens[-1]).split(',')
                for i in range(len(lines_of_sql)):
                    try:
                        res = re.search("(\\'.*\\')", lines_of_sql[i])
                        lines_of_sql[i] = (re.sub("'", '', res.group(0)))
                    except AttributeError:
                        lines_of_sql[i] = None
                result[row[3][1]] = lines_of_sql
        return result

    def _extract_deleted_schemas(self, fpage):
        offset = 100
        cell_offset = unpack('>H', fpage[offset + 8: offset + 10])[0]
        i = 0
        result = {}
        while cell_offset:
            freeblock_size = unpack('>H', fpage[cell_offset + 2: cell_offset + 4])[0]
            one_schema = fpage[cell_offset:cell_offset+freeblock_size]
            res = self._erase_symbols_from_sql_statement(str(one_schema))
            tqdm.write(res)
            result[cell_offset] = res
            i += 2
            cell_offset = unpack('>H', fpage[offset + 8 + i: offset + 10 + i])[0]
        return result

    """This function link all page types together with the schema types.
    With this information it is possible to process the correct schema to the corresponding freeblock and page."""
    def _connect_schema_and_pages(self):
        result = {}
        #TODO: this function search only one level in depth
        for k in self.parsed_schemas:
            result[k] = []
            if k > 0:
                with open(self.filename, "rb") as f:
                    f.seek((k-1) * self.page_size)
                    current_page = f.read(self.page_size)
                    page_header = unpack('>bhhHbL', current_page[:12])
                    """check if the current page is a interior b-tree page"""
                    if page_header[0] == 5:
                        result[k].append(page_header[5])
                        num_of_cells_in_frame = page_header[2]
                        """process the cell-offset array to get the leaf page numbers"""
                        for x in range(num_of_cells_in_frame):
                            start_of_cell_parsing = 12 + (x * 2)
                            cell_offset = unpack('>h', current_page[start_of_cell_parsing: start_of_cell_parsing + 2])[0]

                            cell = unpack('>I', current_page[cell_offset:cell_offset + 4])
                            result[k].append(cell[0])
                    else:
                        result[k].append(k)
        for k in result:
            result[k].sort()

        return result

    """clean the sql-create strings and extract only the needed schemas"""
    def _erase_symbols_from_sql_statement(self, s):
        try:
            result = re.search(r'\bINT\b|\bINTEGER\b|\bBLOB\b|\bTEXT\b|\bREAL\b|\bFLOAT\b|\bBOOL\b|\bBOOLEAN\b', s).group(0)

        except AttributeError:
            return
        result = re.sub('\n', '', result)
        result = re.sub('\t', '', result)
        result = re.sub(' ', '', result)

        result = re.sub('INTEGER', 'INT', result)
        result = re.sub('FLOAT', 'REAL', result)
        result = re.sub('BOOLEAN', 'BOOL', result)
        return result

    def get_page_size(self, fname):
        with open(fname, "rb") as f:
            sqlite_string = f.read(16)
            if not sqlite_string == self.MAGIC_NUMBER:
                return

            f.seek(16)
            header = f.read(self.HEADER_SIZE - 44)
            sqlh = unpack('>HBBBBBBIIIIIIIIIIII', header)
            psize = sqlh[0]
            if psize == 1:
                return 65536
            else:
                return psize
