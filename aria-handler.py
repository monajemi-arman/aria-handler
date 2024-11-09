import pymssql
import json
import sys

# Path to config JSON
config_json = 'config.json'

class AriaHandler:
    def __init__(self, config_json=config_json):
        self.connection = None
        with open(config_json) as f:
            self.config = json.load(f)
        # Check config
        params = ['server', 'username', 'password', 'database']
        for param in params:
            if param not in self.config.keys():
                raise Exception("Wrong keys in config! The following should exist: " + str(params))
        # Connect
        self.connect()

    def connect(self):
        try:
            self.connection = pymssql.connect(server=self.config['server'], user=self.config['username'],
                                              password=self.config['password'], database=self.config['database'])
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

    def exec(self, command, arguments=None, mute=False):
        cursor = self.connection.cursor()
        if arguments:
            cursor.execute(command, arguments)
        else:
            cursor.execute(command)
        if not mute:
            rows = []
            row = cursor.fetchone()
            while row:
                rows.append(row)
                row = cursor.fetchone()
            # Commit changes
            self.connection.commit()
            return rows

    def get_price(self, product_id):
        command = "SELECT TOP (1000) [Price] FROM [Drug].[dbo].[Kala] WHERE [Code] = %d"
        arguments = product_id
        rows = self.exec(command, arguments)
        return rows[0][0]

    def get_stock(self, product_id):
        command = """
            exec sp_executesql N'
            DECLARE @Mojodi float;
            SET @Mojodi = (SELECT Mojodi FROM Mojodi WHERE code = @P1 AND AnbCode = dbo.iszero(@P2, 1));
            SELECT @Mojodi AS Mojodi;', N'@P1 int, @P2 int', %d, 1
        """
        arguments = product_id
        rows = self.exec(command, arguments)
        return rows[0][0]

    def update_facheader_code(self, facheader_id, code=55):
        command = """
            exec sp_executesql N'UPDATE DRUG.dbo.FacHeder
            SET CodeSazeMan = @P1, BimarName = @P2, 
            SazemanIsAzad = @P3
            WHERE CodeFacHeder = @P4',N'@P1 int,@P2 varchar(200),@P3 bit,@P4 int',%d,'0',0,%d
        """
        arguments = (code, facheader_id)
        return self.exec(command, arguments, mute=True)

    def noskhe_to_facheader(self, noskhe_id):
        command = "SELECT TOP (2) CodeFacHeder FROM [Drug].[dbo].[FacHeder] WHERE Sh_Noskhe = %d"
        arguments = noskhe_id
        rows = self.exec(command, arguments)
        return rows[0][0]


def main():
    ah = AriaHandler(config_json)
    print(ah.update_facheader_code(275229))

if __name__ == '__main__':
    main()