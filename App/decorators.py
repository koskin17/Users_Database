import inspect
import pandas as pd
from functools import wraps
from PyQt5.QtWidgets import QMessageBox

import logging

def for_data_about_users(func):
    """Decorator for loading and cleaning user data"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_and_clean_users()

        if df.empty:
            logging.warning("Warning! After cleaning database is empty. Data about users cannot be generated.")
            QMessageBox.warning(self, "Warning!", "After cleaning database is empty.")
            return
        try:
            sig = inspect.signature(func)
            accepts_var_pos = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
            if accepts_var_pos or len(sig.parameters) > 2:
                result = func(self, df, *args, **kwargs)
            else:
                result = func(self, df)
        finally:
            # let Python free local variables; avoid explicit gc.collect()
            pass
        return result
    
    return wrapper

def for_data_about_scans(func):
    """Decorator for loading data about scans"""

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        df: pd.DataFrame = self.load_data_about_scans()

        if df.empty:
            logging.warning("Warning! Database about scans is empty. Data about scans generated cannot be generated.")
            QMessageBox.warning(self, "Warning!", "Database about scans is empty.")
            return
        try:
            sig = inspect.signature(func)
            accepts_var_pos = any(p.kind == p.VAR_POSITIONAL for p in sig.parameters.values())
            if accepts_var_pos or len(sig.parameters) > 2:
                result = func(self, df, *args, **kwargs)
            else:
                result = func(self, df)
        finally:
            # let Python free local variables; avoid explicit gc.collect()
            pass

        return result
    
    return wrapper