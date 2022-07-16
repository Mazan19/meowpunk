"""
Custom exception based on OperationalError
"""
from sqlite3 import OperationalError


class WrongDBPathError(OperationalError):
    def __init__(self, path=None):
        self.message = f"Unable to create or find file {path}"
        super().__init__(self.message)
