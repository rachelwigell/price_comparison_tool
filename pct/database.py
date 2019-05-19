import psycopg2 as pg
import pandas.io.sql as pdsql
from sqlalchemy import create_engine


class Database(object):
    """
    Define connection parameters to a database
    """

    def __init__(self, host='localhost', port=5432, database='postgres', user='postgres', password=''):
        """
        Instantiate an instance of a Database with the given connection parameters.
        :param host: The db host
        :param port: The db port
        :param database: The database name
        :param user: The db user name
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.engine = create_engine('postgresql://{user}:{pw}@{host}:{port}/{db}'
                                    .format(user=user, pw=password, host=host, port=port, db=database))

    def __connect(self):
        """
        Connects to the database using the given parameters
        :return: a psycopg connection object
        """

        return pg.connect(host=self.host, port=self.port, database=self.database, user=self.user)

    def execute(self, sql_string, connection=None, cursor=None):
        """
        Opens a connection to this database, executes the given SQL string, then closes the connection.
        For write-only queries. This function does not return any query results.
        :param sql_string: The SQL string to execute
        :param connection: The database connection, if any. Leave as None to open a new one.
        :param cursor: THe database cursor, if any. Leave as None to instantiate a new one.
        """

        if not connection:
            connection = self.__connect()
        if not cursor:
            cursor = connection.cursor()
        try:
            cursor.execute(sql_string)
        except pg.Error as e:
            cursor.close()
            raise e

    def run(self, sql_string, connection=None):
        """
        Opens a connection to this database and executes the given SQL string.
        Use for select queries, as it returns the query results.
        :param sql_string: The SQL string to execute.
        :param connection: The database connection, if any. Leave as None to open a new one.
        :return: The query result
        """
        if not connection:
            connection = self.__connect()
        return pdsql.read_sql(sql_string, connection)

    def create_table_from_dataframe(self, table_name, df, schema_name='public'):
        df.to_sql(table_name, self.engine, schema=schema_name, if_exists='replace', index=False)
