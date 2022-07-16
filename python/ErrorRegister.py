import sqlite3
from errors_events import ERROR_EVENTS
from WrongDBPathError import WrongDBPathError
from datetime import datetime
from pandas import read_csv as pd_read_csv


class ErrorsRegister():
    """
    Class for Errors registering in SQLIte db
    """

    @staticmethod
    def config_to_sql_string(table_name, cols_dict):
        """
        Compile table configuration structure to CREATE TABLE sql string
        """

        sql = f"CREATE TABLE IF NOT EXISTS {table_name}  ("
        sql += ",".join([f"\r\n{col_name} {col_type}"
                        for col_name, col_type in cols_dict.items()])
        sql = sql + ");"
        return sql

    @staticmethod
    def read_csv(file_path):
        """
        Read CSV file using pandas.read_csv
        Reusing pandas for setting any default flags
        """

        return pd_read_csv(filepath_or_buffer=file_path, sep=",", header=0)

    @staticmethod
    def read_client_or_server_file(file_path):
        """
        Read client.csv of server.csv
        Setting index for further join
        """

        return ErrorsRegister.read_csv(file_path).set_index("error_id")

    def __init__(self, db_path) -> None:
        """
        Constructor
        1) Create sqlite connection
        2) Create sqlite cursor for connection
        3) Read cheaters table in memory for faster filtering

        """

        try:
            self.cheaters_db = sqlite3.connect(f"{db_path}")
        except sqlite3.OperationalError:
            raise WrongDBPathError(db_path)

        self.cheaters_cursor = self.cheaters_db.cursor()
        # TODO: Add error handling (try, except)
        sql = "select player_id, date(ban_time) from cheaters"
        self.cheaters_cursor.execute(sql)
        cheaters = self.cheaters_cursor.fetchall()

        # Creating dict {player_id:ban_day}
        # datetime.strptime(ban_time,"%Y-%m-%d").date() cause
        # we need to compare in days
        # TODO: check if ban_time format has not changed

        self._bans = {player_id: datetime.strptime(ban_time, "%Y-%m-%d").date()
                      for player_id, ban_time in cheaters}

    def __del__(self):
        """
        Destuctor
        Closing sqlite connection
        """

        self.cheaters_db.close()

    def create_errors_tbl(self, table_name):
        """
        Create table for errors in sqlite db with <table_name> name

        Since  config_to_sql_string return "CREATE OR REPLACE... " sql
        so there is no need to check if table exist
        """

        sql = ErrorsRegister.config_to_sql_string(table_name, ERROR_EVENTS)
        self.cheaters_cursor.execute(sql)
        self.cheaters_db.commit()

    def filter_players_by_ban(self):
        """
        Filtering cheaters
        "day" - additional field to make further calculation easier
        """
        self.data["day"] = self.data.apply(
                           lambda row: datetime.fromtimestamp(row["timestamp"])
                           .date(), axis=1)

        # Filter by condition: player_id is not in cheaters,
        # or ban_time>=timestamp
        # _bans.get(row["player_id"],row["day"]) is equal
        #  to NVL(ban_time, timestamp)
        # which provide us true on condition >=row["day"]

        return (
         self.data[self.data.apply(
          lambda row:
          self._bans.get(row["player_id"], row["day"]) >= row["day"],
          axis=1)]
        )

    @staticmethod
    def task_solve(db_file, client_file, server_file, table_name=None):
        """
        Sequence of actions for resolving task
        params db_file,client_file, server_file are mandatory

        """

        if table_name is None:
            table_name = "players_errors"

        errorsRegister = ErrorsRegister(db_file)
        errorsRegister.create_errors_tbl(table_name)
        errorsRegister.data = (
            ErrorsRegister.read_client_or_server_file(client_file)
        )

        errorsRegister.data = (
            errorsRegister.data
            # join on reading table for possible decreasing memory usage
            .join(ErrorsRegister.read_client_or_server_file(server_file),
                  how='inner', lsuffix="_client", rsuffix="_server")
            # reset index to use it like a column
            .reset_index(level=0)
            # select only nessesary columns
            [["timestamp_server", "player_id", "error_id",
              "description_server", "description_client"]]
            # rename for easy saving in db
            .rename(columns={"timestamp_server": "timestamp",
                             "player_id_client": "player_id",
                             "description_server": "json_server",
                             "description_client": "json_client"})
            )

        errorsRegister.data_filtered = errorsRegister.filter_players_by_ban()
        (
            errorsRegister.data_filtered
            # Column [day] was added so there is need to direct specify columns
            # TODO: check if table schema is match to data
            [["timestamp", "player_id", "error_id", "json_server",
             "json_client"]]
            .to_sql(name=table_name, con=errorsRegister.cheaters_db,
                    if_exists='replace', index=False)
        )
        errorsRegister.data_filtered.info()
        print(errorsRegister.data_filtered)
        return errorsRegister
