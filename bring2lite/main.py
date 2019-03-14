from classes.gui import GUI
import logging


def main():
    logger = logging.getLogger('parser')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('debug.log')
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    my_gui = GUI()
    my_gui.process()

if __name__ == "__main__":
    main()
