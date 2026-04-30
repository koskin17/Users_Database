from PyQt5.QtWidgets import QApplication
from PyQt5.QtGui import QFont
from classes import MainWindow
import sys
import logging


logging.basicConfig(
    filename=  "app.log",
    filemode = "a",
    format = "%(asctime)s - %(levelname)s - %(message)s",
    level = logging.INFO
)


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 10, 30, False))
    main_window.show()
    sys.exit(app.exec())
