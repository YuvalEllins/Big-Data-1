from hw1  import DatabaseManager
#CHIGHI PUT YOUR STUFF HERE

DRIVER = "ODBC Driver 18 for SQL Server"   
SERVER = "132.72.64.124"
USERNAME = "idanduha"
PASSWORD = "09iFWp3oyK"
DATABASE = "idanduha" 

def get_db():
    return DatabaseManager(
        driver=DRIVER,
        server=SERVER,
        username=USERNAME,
        password=PASSWORD,
        database=DATABASE,
    )


def reset_db():
    """
    Clear tables so every test starts from a clean state.
    """
    db = get_db()
    conn = db._connect()
    cur = conn.cursor()
    try:
        # First clear AnnualSummary (FK to Films), then Films
        cur.execute("DELETE FROM AnnualSummary;")
        cur.execute("DELETE FROM Films;")
        conn.commit()
    finally:
        cur.close()
        conn.close()


def seed_films_simple():
    """
    Insert a small, known dataset into Films.
    """
    db = get_db()
    conn = db._connect()
    cur = conn.cursor()
    try:
        # year 2000
        cur.execute("INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                    ("Movie A", 2000, 7.5))
        cur.execute("INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                    ("Movie B", 2000, 9.0))

        # year 2001
        cur.execute("INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                    ("Movie C", 2001, 8.1))
        cur.execute("INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                    ("Movie D", 2001, 8.9))
        cur.execute("INSERT INTO Films (TITLE, PROD_YEAR, RATING) VALUES (?, ?, ?)",
                    ("Movie E", 2001, 5.0))

        conn.commit()
    finally:
        cur.close()
        conn.close()


def test_add_summary_and_get_best_films():
    reset_db()
    seed_films_simple()

    db = get_db()
    db.add_summary_items()

    best = db.get_best_films()
    print("get_best_films() =>", best)

    # expectations from seed:
    # 2000 -> Movie B (9.0)
    # 2001 -> Movie D (8.9)
    assert best[2000] == "Movie B"
    assert best[2001] == "Movie D"


def test_get_n_best_years():
    reset_db()
    seed_films_simple()

    db = get_db()
    db.add_summary_items()

    # counts of rating>8:
    # 2000: only Movie B -> 1
    # 2001: Movie C & D -> 2
    years = db.get_n_best_years(2)
    print("get_n_best_years(2) =>", years)

    assert years[0] == 2001      # most films > 8
    assert years[1] == 2000      # second place


def test_file_to_database():
    """
    Optional: test reading from CSV.
    Assumes films.csv is in the same folder.
    """
    reset_db()
    db = get_db()
    db.file_to_database("films.csv")

    # count rows in Films
    conn = db._connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM Films;")
        count = cur.fetchone()[0]
        print("Films rows after file_to_database:", count)
        assert count > 0
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # run tests manually
    test_add_summary_and_get_best_films()
    test_get_n_best_years()
    test_file_to_database()
    print("All tests passed ✅")    
