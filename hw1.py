import pyodbc


class DatabaseManager:
    def __init__(self, driver: str, server: str, username: str, password: str, database: str):
        self.driver = driver
        self.server = server
        self.username = username
        self.password = password
        self.database = database
        

    def _connect(self):
    
        connection_string = (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"

        )
        return pyodbc.connect(connection_string)

    def file_to_database(self, path: str) -> None:
       
        connection = self._connect()
        cursor = connection.cursor()

        try:
            with open(path, "r", encoding="utf-8") as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue

                    # CSV: title,year,rating
                    parts = line.split(",")
                    if len(parts) != 3:
                        continue

                    title = parts[0].strip()
                    prod_year = int(parts[1].strip())
                    rating = float(parts[2].strip())

                    cursor.execute(
                        "INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                        (title, prod_year, rating)
                    )

            connection.commit()
        finally:
            cursor.close()
            connection.close()

    def add_summary_items(self) -> None:
       
        connection = self._connect()
        cursor = connection.cursor()

        try:
            cursor.execute("EXEC AddSummaryItems;")
            connection.commit()
        finally:
            cursor.close()
            connection.close()

    def get_best_films(self) -> dict[int, str]:
     
        connection = self._connect()
        cursor = connection.cursor()

        try:
            cursor.execute(
                """
                SELECT A.PROD_YEAR, F.TITLE
                FROM AnnualSummary AS A
                JOIN Films AS F ON A.FID = F.FID
                """
            )

            result = {}
            for row in cursor.fetchall():
                result[int(row.PROD_YEAR)] = row.TITLE

            return result

        finally:
            cursor.close()
            connection.close()

    def get_n_best_years(self, n: int) -> list[int]:
      
        connection = self._connect()
        cursor = connection.cursor()

        try:
            cursor.execute(
                """
                SELECT PROD_YEAR
                FROM AnnualSummary
                ORDER BY RATED_ABOVE_8 DESC, PROD_YEAR ASC
                """
            )

            years = [int(row.PROD_YEAR) for row in cursor.fetchall()]
            return years[:n]

        finally:
            cursor.close()
            connection.close()
