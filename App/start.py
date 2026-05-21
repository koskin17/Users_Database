from PyQt5.QtWidgets import QApplication
from PyQt5.QtGui import QFont
import sys, os
from dotenv import load_dotenv
import logging

from classes import MainWindow, resource_path

env_path = resource_path(".env")
load_dotenv(env_path)

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
