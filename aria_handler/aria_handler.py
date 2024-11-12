import pymssql
import json
import sys
import time
import threading

# Path to config JSON
config_json = 'config.json'


class AriaHandler:
    def __init__(self, config_json=config_json):
        self.connection = None
        self.keep_alive_thread = None
        self.keep_alive_flag = False
        self.default_code = 55
        with open(config_json) as f:
            self.config = json.load(f)
        # Connect
        self.connect()

    def connect(self, retry_interval=5, max_retries=3):
        """Attempts to connect to the database, and starts a keep-alive thread if successful."""
        attempt = 0
        while attempt < max_retries:
            try:
                self.connection = pymssql.connect(**self.config['mssql'])
                print("Connection established.")
                # Start the keep-alive thread if the first connection succeeds
                if not self.keep_alive_thread or not self.keep_alive_thread.is_alive():
                    self.keep_alive_flag = True
                    self.keep_alive_thread = threading.Thread(target=self.keep_connection_alive, daemon=True)
                    self.keep_alive_thread.start()
                return True
            except pymssql.InterfaceError:
                print("Connection failed: Incorrect server address or network issue.", file=sys.stderr)
            except pymssql.OperationalError as e:
                print("Connection failed: Invalid credentials or database issue.", file=sys.stderr)
                print("Error details:", e)
            attempt += 1
            print(f"Retrying in {retry_interval} seconds... (Attempt {attempt}/{max_retries})")
            time.sleep(retry_interval)

        print("Max retries reached. Could not establish a connection.", file=sys.stderr)
        return False

    def keep_connection_alive(self, check_interval=30):
        """Periodically checks the connection and reconnects if necessary."""
        while self.keep_alive_flag:
            if not self.is_connection_alive():
                print("Connection lost. Reconnecting...")
                self.connect()
            time.sleep(check_interval)

    def is_connection_alive(self):
        """Checks if the connection is alive by executing a simple query."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except (pymssql.InterfaceError, pymssql.OperationalError):
            return False

    def close(self):
        """Stops the keep-alive thread and closes the connection."""
        self.keep_alive_flag = False
        if self.connection:
            self.connection.close()
            print("Connection closed.")

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

    def get_prices(self):
        command = "SELECT [Code],[Price] FROM [Drug].[dbo].[Kala]"
        rows = self.exec(command)
        result = []

        for row in rows:
            if row[0] and row[1]:
                result.append(row)

        return result

    def get_stock(self, product_id):
        command = """
            exec sp_executesql N'
            DECLARE @Mojodi float;
            SET @Mojodi = (SELECT Mojodi FROM Mojodi WHERE code = @P1 AND AnbCode = dbo.iszero(@P2, 1));
            SELECT @Mojodi AS Mojodi;', N'@P1 int, @P2 int', %d, 1
        """
        arguments = product_id
        rows = self.exec(command, arguments)
        stock = rows[0][0]
        if stock < 0:
            stock = 0
        return stock

    def get_stocks(self):
        command = "SELECT code,Mojodi FROM Mojodi"
        rows = self.exec(command)
        result = []

        # Convert stock numbers to integer
        for row in rows:
            stock = int(row[1])
            if stock < 0:
                stock = 0
            new_row = (row[0], stock)
            result.append(new_row)

        return result

    def get_code_stock_prices(self):
        command = """
                SELECT 
                    Mojodi.code,
                    CAST(
                        CASE 
                            WHEN Mojodi.Mojodi < 0 THEN 0 
                            ELSE Mojodi.Mojodi 
                        END AS INT
                    ) AS Mojodi,
                    Kala.Price
                FROM 
                    Mojodi
                JOIN 
                    [Drug].[dbo].[Kala] AS Kala
                ON 
                    Mojodi.code = Kala.Code;
        """
        rows = self.exec(command)
        return rows

    def update_facheader_code(self, facheader_id, code):
        command = """
            exec sp_executesql N'UPDATE DRUG.dbo.FacHeder
            SET CodeSazeMan = @P1, BimarName = @P2, 
            SazemanIsAzad = @P3
            WHERE CodeFacHeder = @P4',N'@P1 int,@P2 varchar(200),@P3 bit,@P4 int',%d,'0',0,%d
        """
        arguments = (code, facheader_id)
        return self.exec(command, arguments, mute=True)

    def user_to_id(self, user):
        command = "SELECT TOP (2) [Id] FROM [Drug].[dbo].[Users] WHERE UserName = %s"
        arguments = str(user)
        rows = self.exec(command, arguments)
        if rows and rows[0] and rows[0][0]:
            return rows[0][0]

    def new_noskhe(self):
        command = """
            exec sp_executesql N'
            DECLARE @PerCode INT, @userId INT, @operation CHAR, @TestMode BIT, @OutOfPlace int;
            DECLARE @limit INT, @MC INT = 1500, @MI INT = 1750, @CodeInsert INT;
            DECLARE @state TABLE(state VARCHAR(3) COLLATE arabic_ci_as);
            
            SET @PerCode = @P1;
            SET @userId = @P2;
            SET @operation = @P3;
            SET @TestMode = @P4;
            SET @OutOfPlace = @P5;
            
            IF (@OutOfPlace = 1)
                INSERT INTO @state(state) VALUES(''10'')
            ELSE BEGIN
                INSERT INTO @state(state) VALUES(''0''),(''20'')
            END;
            
            IF (@TestMode = 1)
            BEGIN
                SELECT @TestMode = 0
                FROM Facheder
                WHERE [State] IN (SELECT state FROM @state)
                HAVING COUNT(*) < @MC AND MAX(codefacheder) < @MI;
            END;
            
            IF (@Operation = ''I'')
            BEGIN
                IF (@TestMode = 0)
                BEGIN
                    SELECT @CodeInsert = ISNULL(MAX(Sh_Noskhe), 0) + 1
                    FROM FacHeder
                    WHERE [State] IN (SELECT state FROM @state);
            
                    INSERT INTO FacHeder (Sh_Noskhe, [State], CodeUser)
                    VALUES (@CodeInsert, ''0'', @userId);
            
                    SELECT @CodeInsert AS Code;
                END
                ELSE
                    SELECT MAX(sh_noskhe) AS Code
                    FROM FacHeder
                    WHERE State IN (SELECT state FROM @state)
                    AND (codefacheder < @MI)
                    AND (@userId = 0 OR codeuser = @userId);
            END
            ELSE BEGIN
                IF (@operation = ''P'')
                    SELECT @limit = MIN(sh_noskhe)
                    FROM FacHeder
                    WHERE State IN (SELECT state FROM @state)
                    AND (@userId = 0 OR codeuser = @userId);
            
                IF (@operation = ''N'')
                    SELECT @limit = MAX(sh_noskhe)
                    FROM FacHeder
                    WHERE State IN (SELECT state FROM @state)
                    AND (@TestMode = 0 OR codefacheder < @MI)
                    AND (@userId = 0 OR codeuser = @userId);
            
                SELECT CASE
                           WHEN @operation = ''F'' THEN ISNULL(MIN([sh_noskhe]), 0)
                           WHEN @operation = ''L'' THEN ISNULL(MAX([sh_noskhe]), 0)
                           WHEN @operation = ''P'' THEN ISNULL(MAX([sh_noskhe]), @limit)
                           WHEN @operation = ''N'' THEN ISNULL(MIN([sh_noskhe]), @limit)
                           WHEN @operation = ''S'' THEN ISNULL(MAX(sh_noskhe), -1)
                           ELSE -1
                       END AS Code
                FROM FacHeder
                WHERE State IN (SELECT state FROM @state)
                AND (@userId = 0 OR codeuser = @userId)
                AND (@operation <> ''P'' OR sh_noskhe < @PerCode)
                AND (@operation <> ''N'' OR sh_noskhe > @PerCode)
                AND (@operation <> ''F'' OR sh_noskhe > 0)
                AND (@operation <> ''S'' OR sh_noskhe = @PerCode)
                AND (@TestMode = 0 OR codefacheder < @MI);
            END;', 
            N'@P1 int, @P2 int, @P3 varchar(8000), @P4 bit, @P5 int', 0, %d, 'I', 0, 0
        """
        arguments = self.config['user_id']
        rows = self.exec(command, arguments)
        return rows[0][0]

    def noskhe_to_facheader(self, noskhe_id):
        command = "SELECT TOP (2) CodeFacHeder FROM [Drug].[dbo].[FacHeder] WHERE Sh_Noskhe = %d"
        arguments = noskhe_id
        rows = self.exec(command, arguments)
        return rows[0][0]

    def add_to_fac(self, fac_id, product_id, amount=1):
        command = """
            EXEC sp_executesql 
            N'INSERT INTO DRUG.dbo.facradif 
                (CodeFacHeder, Code, CodeM, Per, Ted, Gh, State, JamRadif, 
                 EzafeDariafty, SahmBimar, Flag, TedM, GhM, Rdf, AnbCode, 
                 Flag2, CodeDastorDarie, DarsadTakhfif, fk_codetarkib, codeuser)
             VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, 
                     @P11, @P12, @P13, @P14, @P15, @P16, @P17, @P18, 
                     @P19, @P20);
             SELECT Buy, SCOPE_IDENTITY() AS CodeRadifFac 
             FROM DRUG.dbo.facradif 
             WHERE CodeRadifFac = SCOPE_IDENTITY()',
            N'@P1 int, @P2 int, @P3 int, @P4 float, @P5 float, @P6 float, 
              @P7 varchar(4), @P8 float, @P9 float, @P10 float, @P11 int, 
              @P12 float, @P13 float, @P14 int, @P15 int, @P16 int, 
              @P17 int, @P18 float, @P19 int, @P20 int', 
            %d, %d, 0, -1, %d, %d, '0', 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, %d
        """
        price = self.get_price(product_id)
        arguments = (fac_id, product_id, amount, price, self.config['user_id'])
        return self.exec(command, arguments)

    def add_products_to_noskhe(self, products, noskhe_id=None, code=None):
        """
        Batch add products to noskhe
        :param products: a tuple of tuples like: ((product_id, amount), (product_id, amount), ...)
        :param noskhe_id: ID of noskhe
        :return:
        """
        # Generate new noskhe if not present
        if not noskhe_id:
            noskhe_id = self.new_noskhe()
            print(noskhe_id)
        # Code. 55 is by default for OTC
        if not code:
            code = self.default_code

        # Find factor id based on noskhe
        fac_id = self.noskhe_to_facheader(noskhe_id)
        # Apply code on factor
        self.update_facheader_code(fac_id, code)

        # Add per product
        result = []
        for product_amount in products:
            result.append(
                self.add_to_fac(fac_id, *product_amount)
            )
        return result
