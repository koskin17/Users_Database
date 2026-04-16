import gc
import pandas as pd
import inspect
from functools import wraps
from PyQt5.QtWidgets import QMessageBox

def for_data_about_users(func):
    """Decorator for loading and cleaning user data"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_and_clean_users()
        
        if df.empty:
            QMessageBox.warning(self, "Warning!", "After cleaninf database is empty.")
            return
        try:
            sig = inspect.signature(func)
            accepts_var_pos = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
            if accepts_var_pos:
                result = func(self, df, *args, **kwargs)
            else:
                result = func(self, df, **kwargs)
        finally:
            del df
            gc.collect()
            print("DataFrame is deleted") #TODO delete after cleaning
        
        return result
    
    return wrapper

def for_data_about_scans(func):
    """Decorator for loading data about scans"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_data_about_scans()
        
        if df.empty:
            QMessageBox.warning(self, "Warning!", "Database about scans is empty.")
            return
        try:
            sig = inspect.signature(func)
            accepts_var_pos = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
            if accepts_var_pos:
                result = func(self, df, *args, **kwargs)
            else:
                result = func(self, df, **kwargs)
        finally:
            del df
            gc.collect()
            print("DataFrame is deleted") #TODO delete after cleaning
        
        return result
    
    return wrapper