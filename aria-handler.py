import pymssql
import json
import sys

class AriaHandler:
    def __init__(self, config):
        # Check config
        params = ['server', 'username', 'password', 'database']
        for param in params:
            if param not in config.keys():
                raise Exception("Wrong keys in config! The following should exist: " + str(params))
        # Connect
        self.connect(*params)

    def connect(self, server, username, password, database):
        try:
            self.connection = pymssql.connect(server=server, user=username, password=password, database=database)
            return True
        except pymssql.InterfaceError:
            print("Connection failed: Incorrect server address or network issue.", file=sys.stderr)
            return False
        except pymssql.OperationalError as e:
            print("Connection failed: Invalid credentials or database issue.", file=sys.stderr)
            print("Error details:", e)
            return False

    def close(self):
        self.connection.close()
