import pyodbc



class DatabaseManager:
    def __init__(self, driver: str, server: str, username: str, password: str, database: str):
        pass  # TODO

    def file_to_database(self, path: str) -> None:
        pass  # TODO

    def add_summary_items(self) -> None:
        pass  # TODO

    def get_best_films(self) -> dict[int, str]:
        pass  # TODO

    def get_n_best_years(self, n: int) -> list[int]:
        pass  # TODO
