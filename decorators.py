import gc
import pandas as pd
import inspect
from functools import wraps
from PyQt5.QtWidgets import QMessageBox

import logging

def for_data_about_users(func):
    """Decorator for loading and cleaning user data"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_and_clean_users()
        
        if df.empty:
            logging.warning("Warning! After cleaning database is empty. Data about users cannot be generated.", exc_info = True)
            QMessageBox.warning(self, "Warning!", "After cleaning database is empty.")
            return
        try:
            sig = inspect.signature(func)
            accepts_var_pos = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
            if accepts_var_pos:
                result = func(self, df, *args, **kwargs)
            else:
                result = func(self, df, **kwargs)
        finally:
            try:
                logging.info("Cleaning up user data from memory.", exc_info = True)
                del df
                gc.collect()
                logging.info("User data has been cleaneв from memory.", exc_info = True)
            except Exception as e:
                logging.error("Error during cleaning up user data from memory.", exc_info = True)        
        return result
    
    return wrapper

def for_data_about_scans(func):
    """Decorator for loading data about scans"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_data_about_scans()
        
        if df.empty:
            logging.warning("Warning! Database about scans is empty. Data about scans generated cannot be generated.", exc_info = True)
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
            try:
                logging.info("Cleaning up scan data from memory.", exc_info = True)
                del df
                gc.collect()
                logging.info("Scan data has been cleaned from memory.", exc_info = True)
            except Exception as e:
                logging.error("Error during cleaning up scan data from memory.", exc_info = True)
        
        return result
    
    return wrapper