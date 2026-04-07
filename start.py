from PyQt5.QtWidgets import QApplication
from classes import *
import sys


''' Start main program '''
if __name__ == '__main__':
    # Create application app.
    app = QApplication(sys.argv)
    # Create main window for all other widgets.
    main_window = MainWindow()
    main_window.setFont(QFont('Font/pfdintextpro-thinitalic.ttf', 10, 30, False))
    # Showing the main windows
    main_window.show()
    # Start app with method exec_, which starting loop
    sys.exit(app.exec())
