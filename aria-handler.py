import pymssql
import json
import sys

# Path to config JSON
config_json = 'config.json'


class AriaHandler:
    def __init__(self, config_json=config_json):
        self.connection = None
        self.default_code = 55
        with open(config_json) as f:
            self.config = json.load(f)
        # Connect
        self.connect()

    def connect(self):
        try:
            self.connection = pymssql.connect(**self.config['mssql'])
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

def main():
    ah = AriaHandler(config_json)
    print(ah.add_products_to_noskhe(((11083, 3),)))


if __name__ == '__main__':
    main()
