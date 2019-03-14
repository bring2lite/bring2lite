from .parser import Parser
from .report_generator import ReportGenerator
import logging
import binascii
import os
from struct import *
import hashlib


d = {}
class WALParser(Parser):
    MAGIC_NUMBERS = [
        binascii.unhexlify(b'377f0682'),
        binascii.unhexlify(b'377f0683')
    ]
    FILE_FORMAT_VERSION = binascii.unhexlify(b'03007000')
    """defining the offsets which have to be added by processing each page"""
    WAL_HEADER_SIZE = 32
    FRAME_HEADER_SIZE = 24
    MAIN_DB_HEADER_SIZE = 100

    def __init__(self):
        self.logger = logging.getLogger("parser.WALParser")
        self.filename = ""
        self.rgen = ReportGenerator()

    def parse(self, filename, outname, format, sqlite_present=False):
        #tqdm.write(Fore.GREEN + "[+] " + "Start parsing main WAL file")
        self.filename = filename
        self.outname = outname
        # CSV = 0 | XML = 1 | JSON = 2
        self.forma = format
        if not os.path.isfile(self.filename):
            raise IOError
        self.filesize = os.stat(self.filename).st_size
        self._parse_header()
        self._parse_body(sqlite_present)
        return d

    def _parse_header(self):
        with open(self.filename, "rb") as f:
            self.logger.debug("read WAL header")
            header = f.read(32)

        self.wal_header = unpack('>IIIIIIII', header)
        self.logger.debug("extract header data: " + str(self.wal_header))
        self.magic = self.wal_header[0]
        self.page_size = self.wal_header[2]
        self.checkpoint_sequence = self.wal_header[3]
        self.salt_1 = self.wal_header[4]

        self.logger.debug("end of WAL header parsing")

    def _parse_body(self, sqlite_present):
        self.framecount = int((self.filesize - self.WAL_HEADER_SIZE) / (self.FRAME_HEADER_SIZE + self.page_size))
        main_hashes = []
        global d
        d["wal"] = {}
        if sqlite_present:
            main_hashes = self._extract_sqlite_hashes()
        with open(self.filename, "rb") as f:
            self.logger.debug("read WAL body ")
            """loop over each page within the WAL"""
            for i in range(0, self.framecount):
                """calculating the offset to each frame with the defined statics"""
                frame_offset = i*(self.page_size + self.FRAME_HEADER_SIZE) + self.WAL_HEADER_SIZE
                f.seek(frame_offset)
                #TODO: maybe add the page number to the output
                frame_page_number = unpack('>I', f.read(4))

                f.seek(frame_offset + self.FRAME_HEADER_SIZE)
                p = f.read(self.page_size)
                if hashlib.sha256(p).hexdigest() in main_hashes:
                    continue

                result = super(WALParser, self)._parse_page(p, is_wal=True)
                if not isinstance(result, int):
                    d["wal"][i] = result
                    self.rgen.generateReport(os.path.abspath(self.outname + "/" + super(WALParser, self)._path_leaf(self.filename) + "/WALs/" + "/"),
                                                 str(i) + "-wal-frame", result)

        self.logger.debug("end of WAL body parsing")


    """
    Experiment to look for active and inactive pages in the main file to reduce parsing time
    """
    def _extract_sqlite_hashes(self):
        main_page_hashes = []
        with open(self.filename[0:-4], "rb") as f:
            f.seek(16)
            header = f.read(self.MAIN_DB_HEADER_SIZE - 44)
            sqlite_header = unpack('>HBBBBBBIIIIIIIIIIII', header)
            page_size = sqlite_header[0]
            if page_size == 1:
                page_size = 65536
            file_size = os.stat(self.filename[0:-4]).st_size
            max = file_size / page_size
            for counter in range(1, int(max)):
                f.seek(counter * page_size)
                one_page = f.read(page_size)
                main_page_hashes.append(hashlib.sha256(one_page).hexdigest())

        return main_page_hashes
