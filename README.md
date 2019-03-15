# bring2lite
## About
The tool was developed to process SQLite databases in respect of deleted records. Therefore, bring2lite is able to analyse the structures within the main database, WAL files and journal files.
## Installation

## Usage

- **Process a single database main file:**
````bash
main.py --filename /path/to/file --out /path/to/output/folder
````

- **Process a single journal file:**
````bash
main.py --journal /path/to/journal/file --out /path/to/output/folder
````

- **Process a single database main file:**
````bash
main.py --wal /path/to/journal/file --out /path/to/output/folder
````

- **Process all files within a single folder and all sub-folders:**
````bash
main.py --folder /path/to/folder --out /path/to/output/folder
````


## Used libraries
- [tqdm](https://github.com/tqdm/tqdm) - a library which can be used to created progress bars
- [sqlparse](https://github.com/andialbrecht/sqlparse) - this library allows to easily process SQLite statements
- [pyqt5](https://github.com/andialbrecht/sqlparse) and [tkinter](https://github.com/andialbrecht/sqlparse) - libraries which allow to display the processed results within a GUI

## Changelog
- 14-03-2019 - publication of version 0.1

## Tasklist
- [ ] Better error handling
- [ ] Display all processed informations in a interactive GUI

