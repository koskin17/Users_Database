from PyQt5.QtWidgets import QApplication
from classes import *
import sys


if __name__ == '__main__':
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 10, 30, False))
    main_window.show()
    sys.exit(app.exec())
