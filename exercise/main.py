import sqlite3
from pathlib import Path

db_path = Path(Path(__file__).parent, "example.db")

if db_path.exists():
    db_path.unlink()

conn = sqlite3.connect(db_path)


def create_transcations_table(conn=conn):
    with conn:
        conn.execute(
            """
            CREATE TABLE Transactions (
            TransactionID NUM DEFAULT 1 PRIMARY KEY ASC UNIQUE NOT NULL,
            CustomerID NUM NOT NULL,
            TransactionTypeID NUM NOT NULL,
            TransactionDate TEXT NOT NULL,
            Amount REAL,
            ModifyDate TEXT DEFAULT DATE, /* not sure if this works the way expected... */
            ModifyUser TEXT,
            CreateDate TEXT DEFAULT DATE NOT NULL,
            CreateUser TEXT NOT NULL
            )
            """
        )


def create_customer_table(conn=conn):
    with conn:
        conn.execute(
            """
            CREATE TABLE Customer (
            CustomerID NUM DEFAULT 10 PRIMARY KEY ASC UNIQUE NOT NULL, /* might have to use generated table for ASC rule */
            CustomerName TEXT NOT NULL,
            Street TEXT,
            City TEXT,
            State TEXT,
            Country TEXT,
            Zip TEXT,
            CreditLimit REAL,
            ModifyDate TEXT,
            ModifyUser TEXT,
            CreateDate TEXT NOT NULL,
            CreateUser TEXT NOT NULL
            )
            """
        )


def create_dim_date_table(conn=conn):
    with conn:
        conn.execute(
            """
            CREATE TABLE DimDate (
            Year NUM,
            Month NUM,
            Day NUM,
            Hour NUM DEFAULT 0,
            Minute NUM DEFAULT 0,
            Second NUM DEFAULT 0
            )
            """
        )

        # TODO: seed with actual dates


def create_dim_transaction_type_table(conn=conn):
    with conn:
        conn.execute(
            """
            CREATE TABLE DimTransactionType (
            TransactionTypeID NUM NOT NULL,
            TransactionName TEXT NOT NULL,
            StartDate TEXT DEFAULT "1900-01-01",
            EndDate TEXT DEFAULT "2500-01-01",
            ModifyDate TEXT,
            ModifyUser TEXT,
            CreateDate TEXT NOT NULL,
            CreateUser TEXT NOT NULL
            )
            """
        )


def create_tables():

    create_transcations_table()
    create_customer_table()
    create_dim_date_table()
    create_dim_transaction_type_table()


def find_distinct_transaction_type_ids(conn=conn):
    """ #2 """
    with conn:
        rows = conn.execute(
            """
            SELECT TransactionTypeID as transaction_type_id
            FROM Transactions, DimTransactionType
            on Transactions.TransactionTypeID = DimTransactionType.TransactionTypeID
            where DimTransactionType.TransactionTypeID IS NULL
            """
        ).fetchall()

        # rows is a tuple with only one element
        return [r[0] for r in rows]


def get_transaction_amount_by_state(conn=conn):
    """#4"""

    # since we don't have native datetimes in sqlite
    # we have to come up with the month pattern ourselves and perform
    # the necessary string operations for this query

    with conn:

        conn.execute(
            """
            CREATE TABLE ReportingTable as (
            SELECT C.State as State, SUM(T.amount) as amount
            FROM Transactions T, Customer C
            ON T.CustomerID = C.CustomerID
            WHERE T.CreateDate LIKE "2018-{month_pattern}-%"
            )
            """.format(
                month_pattern=...
            )
        )

def customer_activity_in_last_18_months(conn=conn):
    """ #5 """

    # same as before, no native range operations so we're stuck with using
    # string interpolation
    result = []

    with conn:
        for year in ['2019', '...']:
            for month in ['01', '...']:
                result.append(
                    conn.execute(
                        """
                        SELECT c.State as State, DISTINCT(c.CustomerID) as customer, count(t.TransactionID) as total_transactions
                        FROM Customer c, Transactions t
                        ON c.CustomerID = t.CustomerID
                        WHERE t.CreateDate LIKE '{year}-{month}'
                        GROUP BY c.State, c.CustomerID
                        """.format(year=year, month=month)
                    ).fetchall()
                )

    return result

def update_zip_codes(conn=conn):
    " #7 "
    with conn:
        conn.execute(
            """
            UPDATE Customer c
            SET c.ZIP = NULL
            WHERE INT(c.ZIP) < 9999
            or INT(c.ZIP) > 99999 
            """
        )

if __name__ == "__main__":
    create_tables()
