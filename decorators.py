import gc
import pandas as pd
from functools import wraps
from PyQt5.QtWidgets import QMessageBox

def for_data_about_users(func):
    """Decorator for loading and cleaning user data"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_and_clean_users()
        
        if df.empty:
            QMessageBox.warning(self, "Warning!", "After cleaninf database is empty.")

        try:
            results = func(self, df, *args, **kwargs)
        finally:
            del df
            gc.collect()
            print("Daframe is deleted") #TODO delete after cleaning

        return results
    
    return wrapper