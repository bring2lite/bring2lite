import logging
#GUI
import sys
import re
from builtins import print

from PyQt5.QtWidgets import QMainWindow, QApplication, QWidget, QAction, QTableWidget, QTableWidgetItem, QVBoxLayout, \
    QHBoxLayout, QLabel, QGridLayout, QPushButton, QGroupBox, QLineEdit, QPlainTextEdit
from PyQt5 import sip

class Visualizer():
    def __init__(self):
        self.logger = logging.getLogger("parser.SQLiteParser")

    def visualize(self, data):
        #CREATE A WINDOW
        app = QApplication(sys.argv)
        w = MainWindow(data)
        sys.exit(app.exec_())

class MainWindow(QWidget):
    def __init__(self, data):
        super().__init__()
        self.windows = list()
        self.initUI(data)

    def initUI(self, data):
        self.data = data
        self.resize(1000, 500)
        self.move(10, 10)
        self.setWindowTitle('Results')

        #LABELS
        self.label_schema = QLabel("Available schemas:", self)
        self.label_freelists = QLabel("Freelist Pages (Trunk and Leaf):", self)

        #CREATE A TABLE
        self.schema_table = QTableWidget(self)
        #self.schema_table.setHorizontalHeaderLabels(['Root Page'])

        #CALCULATE LONGEST SCHEMA
        longest_schema = 0
        for k, v in self.data["schema"].items():
            if len(v) > longest_schema:
                longest_schema = len(v)

        self.schema_table.setRowCount(len(self.data["schema"]))
        self.schema_table.setColumnCount(longest_schema + 1)

        #INSERT ONE SCHEMA PER ROW
        iter = 0
        try:
            for k, v in self.data["schema"].items():
                self.schema_table.setItem(iter, 0, QTableWidgetItem(str(k)))
                for i, datatype in enumerate(v):
                    if datatype is None:
                        self.schema_table.setItem(iter, i + 1, QTableWidgetItem("ERROR"))
                    else:
                        self.schema_table.setItem(iter, i + 1, QTableWidgetItem(datatype))
                iter += 1
        except KeyError:
            pass
        self.schema_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.schema_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.schema_table.cellDoubleClicked.connect(self._on_row_click)

        #CREATE FREELIST WIDGED
        self.freelist_table = QTableWidget(self)
        longest_freelist_schema = 0
        try:
            for v in self.data['body']['flist-trunk']:
                if len(v) > longest_freelist_schema:
                    longest_freelist_schema = len(v)

            self.freelist_table.setRowCount(len(self.data['body']['flist-trunk']))
            self.freelist_table.setColumnCount(longest_freelist_schema)
            y = 0
            for v in self.data['body']['flist-trunk']:
                x = 0
                for item in v:
                    self.freelist_table.setItem(y, x, QTableWidgetItem(str(item)))
                    x += 1
                y += 1
        except KeyError:
            pass
        self.freelist_table.cellDoubleClicked.connect(self._on_freelist_cell_clicked)
        self.freelist_table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.whatLayout = QGridLayout(self)
        self.whatLayout.addWidget(self.label_schema, 0, 0)
        self.whatLayout.addWidget(self.schema_table, 1, 0)
        self.whatLayout.addWidget(self.label_freelists,2,0)
        self.whatLayout.addWidget(self.freelist_table,3,0)

        self.setLayout(self.whatLayout)
        self.show()

    def _on_row_click(self, row, column):
        self.nwindow = PageWindow(self.data, (self.schema_table.item(row, 0)).text())

    def _on_freelist_cell_clicked(self, row, column):
        self.details = DetailView(self.freelist_table.item(row, column).text())

class PageWindow(QWidget):
    def __init__(self, data, selected_root_page):
        super().__init__()
        self.selected_root_page = int(selected_root_page)
        self.data = data
        self.resize(500, 300)
        self.move(100, 100)
        self.setWindowTitle(selected_root_page)
        self.label_schema = QLabel("Results to the shema with root page: " + str(selected_root_page), self)

        self.result_table_data = QTableWidget(self)
        self.result_table_unallocated = QTableWidget(self)

        longest_schema = len(self.data["schema"][int(selected_root_page)])
        tlength = self._calculate_regular_table_length()
        unalloclength = self._calculate_unalloc_table_length()

        self.result_table_data.setRowCount(tlength)
        self.result_table_data.setColumnCount(longest_schema + 1)
        self.result_table_unallocated.setRowCount(unalloclength)
        self.result_table_unallocated.setColumnCount(longest_schema + 1)

        self.result_table_data.cellDoubleClicked.connect(self._on_regular_cell_click)
        self.result_table_unallocated.cellDoubleClicked.connect(self._on_unalloc_gular_cell_click)

        self.result_table_data.setEditTriggers(QTableWidget.NoEditTriggers)
        self.result_table_unallocated.setEditTriggers(QTableWidget.NoEditTriggers)
        self.header = list()
        self.header.append("Page No.")
        for i, datatype in enumerate(self.data["schema"][int(selected_root_page)]):
            if datatype is None:
                self.header.append("ERROR")
            else:
                self.header.append(datatype)

        self.result_table_data.setHorizontalHeaderLabels(self.header)
        self.result_table_unallocated.setHorizontalHeaderLabels(self.header)

        self._fill_data_table()

        self.lay = QGridLayout(self)
        self.lay.addWidget(self.label_schema, 0, 0)
        self.lay.addWidget(self.result_table_data, 1, 0)
        self.lay.addWidget(self.result_table_unallocated, 2, 0)
        self.setLayout(self.lay)

        self.show()

    def _fill_data_table(self):
        iter_list = self.data["schema_related_pages"][self.selected_root_page]
        counter_regular = 0
        counter_unalloc = 0
        for j, number in enumerate(iter_list):
            for k, v in (self.data["body"][number]).items():
                if k is 'page':
                    for i, entrys in enumerate(v):
                        self.result_table_data.setItem(counter_regular, 0, QTableWidgetItem(str(number)))
                        for l, single_entry in enumerate(entrys):
                            self.result_table_data.setItem(counter_regular, l+1,  QTableWidgetItem(str(single_entry[1])))
                        counter_regular += 1
                elif k is 'unalloc':
                    for i, entrys in enumerate(v):
                        self.result_table_unallocated.setItem(counter_unalloc, 0, QTableWidgetItem(str(number)))
                        for l, single_entry in enumerate(entrys):
                            self.result_table_unallocated.setItem(counter_unalloc, l+1,  QTableWidgetItem(str(single_entry[1])))
                        counter_unalloc += 1

    def _calculate_regular_table_length(self):
        result = 0
        for j, number in enumerate(self.data["schema_related_pages"][self.selected_root_page]):
            for k, v in (self.data["body"][number]).items():
                if k is "page":
                    result += len(v)
        return result

    def _calculate_unalloc_table_length(self):
        result = 0
        for j, number in enumerate(self.data["schema_related_pages"][self.selected_root_page]):
            for k, v in (self.data["body"][number]).items():
                if k is "unalloc":
                    result += len(v)
        return result

    def _on_regular_cell_click(self, row, column):
        self.details = DetailView(self.result_table_data.item(row, column).text())

    def _on_unalloc_gular_cell_click(self, row, column):
        self.details = DetailView(self.result_table_unallocated.item(row, column).text())


class DetailView(QWidget):
    def __init__(self, context):
        super().__init__()
        self.context = context
        self.resize(300, 200)
        self.move(1000, 100)

        self.textbox = QPlainTextEdit(self)
        self.textbox.setPlainText(self.context)
        self.textbox.move(20, 20)
        self.textbox.resize(100, 100)

        self.search_button = QPushButton(self)
        self.search_button.setText("Search")
        self.search_button.clicked.connect(self._search)

        self.search_box = QLineEdit(self)
        self.search_box.returnPressed.connect(self.search_button.click)

        self.lay = QGridLayout(self)
        self.lay.addWidget(self.textbox, 0, 0)
        self.lay.addWidget(self.search_box, 1, 0)
        self.lay.addWidget(self.search_button, 2, 0)
        self.setLayout(self.lay)
        self.show()

    def _search(self):
        searchstring = self.search_box.text()
        #positions = [m.start() for m in re.finditer(searchstring, self.context)]
        self.textbox.find(searchstring)
        self.textbox.setBackgroundVisible(False)

        #print(str(positions))
