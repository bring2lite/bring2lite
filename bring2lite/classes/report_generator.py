import os
import hashlib
from tqdm import tqdm
from colorama import *
import binascii
from tkinter import *
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import askdirectory

class ReportGenerator:
    def __init__(self):
        self.my_path = ""

    def generateReport(self, path, filename, data, format="CSV", schema=["No schema found"]):
        if data is None:
            return

        if not os.path.exists(path):
            os.makedirs(path)

        # if format:
        #     with open(path+filename+'.csv', 'w', newline='') as f:
        #         writer = csv.writer(f)
        #         writer.writerows(data)
        if data:
            out = ""
            for datatype in schema:
                out += str(datatype) + ","
            out += "\n"

            for frame in data:
                if isinstance(frame, list):
                    for y in frame:
                        if self.is_text(y[0]):
                            try:
                                out += str(y[1].decode('utf-8')) + ","
                            except UnicodeDecodeError:
                                out +=str(y[1]) + ","
                                continue
                        else:
                            out += str(y[1]) + ","
                out += "\n"
            out += "++++++++++++++++++++++++++++\n"
            try:
                with open(path + "/" + filename + '.log', "a") as f:
                    f.write(out)
            except UnicodeEncodeError:
                tqdm.write("can not write the record because of unicode errors")

            self.print_hash(path + "/" + filename + '.log')

    def generate_schema_report(self, path, filename, data, csv):
        if data is None:
            return

        if not os.path.exists(path):
            os.makedirs(path)

        out = ""
        with open(path + "/" + filename + '.log', "a") as f:
            for key, value in data.items():
                # out += str(key) + ", "
                if isinstance(value, list):
                    for y in value:
                        out += str(y) + ", "
                out += "\n"
            out += "++++++++++++++++++++++++++++\n"

            f.write(out)

        self.print_hash(path + "/" + filename + '.log')

    def generate_freeblock_report(self, path, filename, freeblocks):
        if freeblocks is None:
            return

        if not os.path.exists(path):
            os.makedirs(path)

        with open(path + "/" + filename + '.log', "a") as f:
            for solutions in freeblocks:
                for s in solutions:
                    if isinstance(s[0], list):
                        f.write(str(s) + ",")
                    else:
                        if self.is_text(s[0]):
                            f.write(str(s[1].decode('utf-8')) + ",")
                        else:
                            f.write(str(s[1]) + ",")
                f.write("\n" + "###################" + "\n")

        self.print_hash(path + "/" + filename + '.log')

    def is_text(self, tester):
        return tester == 'TEXT'

    def print_hash(self, filename):
        with open(filename, "rb") as f:
            d = f.read()
            tqdm.write("sha-256: " + filename + '\t => \t' + str(hashlib.sha256(d).hexdigest()))