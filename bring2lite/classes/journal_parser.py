from .parser import Parser
from .report_generator import ReportGenerator
import logging
import binascii
import os
import re
from struct import *


class JournalParser(Parser):
    MAGIC_NUMBER = binascii.unhexlify(b'd9d505f920a163d7')

    def __init__(self):
        self.logger = logging.getLogger("parser.JournalParser")
        self.rgen = ReportGenerator()

    def parse(self, filename, outname, format, page_size):
        self.filename = filename
        self.outname = outname
        # CSV = 0 | XML = 1 | JSON = 2
        self.format = format
        if not os.path.isfile(self.filename):
            raise IOError
        self.filesize = os.stat(self.filename).st_size
        self.page_size = page_size
        self._parse_header()
        self._parse_body()

    def _parse_header(self):
        self.logger.debug("read journal header ")
        with open(self.filename, 'rb') as f:
            """searching the disk sector sized offset to find content"""
            str = f.read(512)
            counter = 2
            to_test = unpack('>H', str[counter: counter + 2])[0]

            while to_test == 0:
                counter += 2
                try:
                    to_test = unpack('>H', str[counter: counter + 2])[0]
                except error:
                    to_test = None
                    #counter -= 2
            '''
            while to_test is None:
                
                if to_test:
                    break
                counter += 1
            '''
            if counter >= 512 and counter < 1024:
                counter = 512

            self.header_padding = counter

        if self.header_padding == 0:
            with open(self.filename, "rb") as f:
                self.logger.debug("read WAL header")
                header = f.read(28)

            self.journal_header = unpack('>QIIIII', header)
            self.magic = self.journal_header[0]
            self.page_count = self.journal_header[1]
            self.initial_size_of_the_database_in_pages = self.journal_header[3]
            if self.journal_header[4] is not 0:
                self.header_padding = self.journal_header[4]
            if self.page_size is 0:
                self.page_size = self.journal_header[5]
        self.logger.debug("end read journal header ")
        if not self.page_size:
            raise ValueError

    def _parse_body(self):
        checksum_size = pagenumber_size = 4

        self.page_counter = int((self.filesize - self.header_padding) / (checksum_size + pagenumber_size + self.page_size))
        with open(self.filename, "rb") as f:
            self.logger.debug("read journal body ")
            for i in range(0, self.page_counter):
                page_offset = i*(self.page_size + pagenumber_size) + self.header_padding
                if(i > 0):
                    page_offset += i * checksum_size

                f.seek(page_offset)
                page_number = unpack('>I', f.read(4))

                f.seek(page_offset + pagenumber_size)
                page = f.read(self.page_size)

                result = super(JournalParser, self)._parse_page(page)
                if not isinstance(result, int):
                    self.rgen.generateReport(os.path.abspath(self.outname + "/" + super(JournalParser, self)._path_leaf(self.filename) + "/Journals/" + "/"), str(i) +
                                         "-jornal-page", result, self.format, schema=["No schema found"])
        self.logger.debug("end body pardsing")

    def match_zeros(strg, search=re.compile(r'[^0]').search):
        return not bool(search(strg))