import pyodbc



class DatabaseManager:
    def __init__(self, driver: str, server: str, username: str, password: str, database: str):
        connection_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        self.connection = pyodbc.connect(connection_string)
        self.cursor = self.connection.cursor()

    def file_to_database(self, path: str) -> None:
        with open(path, 'r', encoding='utf-8') as file:
            for line in file:
                fields = line.strip().split('\t')
                # Assuming the table has columns matching the fields
                placeholders = ', '.join('?' * len(fields))
                sql = f"INSERT INTO Films VALUES ({placeholders})"
                self.cursor.execute(sql, fields)
        self.connection.commit()

    def add_summary_items(self) -> None:
        with self.connection.cursor as cursor:
            cursor.execute("EXEC AddSummaryItems")
            self.connection.commit()

    def get_best_films(self) -> dict[int, str]:
        with self.connection.cursor() as cursor:
            cursor.execute("EXEC GetBestFilms")
            results = cursor.fetchall()
            return {row.Year: row.Title for row in results}  # type: ignore

    def get_n_best_years(self, n: int) -> list[int]:
        with self.connection.cursor() as cursor:
            cursor.execute("EXEC GetNBestYears ?", n)
            results = cursor.fetchall()
            return [row.Year for row in results]  # type: ignore
