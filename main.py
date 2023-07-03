"""
    console --- python3 main.py people people.csv
"""
from sys import argv

import luigi
import luigi.contrib.postgres as s
import pandas as pd

COUM = []
class CheckTable(s.PostgresQuery):
    host = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()
    password = luigi.Parameter()
    table = luigi.Parameter()

    query = ''

    def run(self):
        connection = self.output().connect()
        connection.autocommit = self.autocommit
        cursor = connection.cursor()
        sql = f"""SELECT column_name, data_type
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = '{self.table}';""" 

        cursor.execute(sql)
        COUM[:] = cursor.fetchall()

        # Update marker table
        self.output().touch(connection)

        # commit and close connection
        connection.commit()
        connection.close()

class ConDatabase(s.CopyToTable):
    host = "localhost"
    database = "my_db"
    user = "root"
    password = "root"
    table = luigi.Parameter()
    file = luigi.Parameter()

    def requires(self):
        return CheckTable(host=self.host,
                         database=self.database,
                         user=self.user,
                         password=self.password,
                         table=self.table)

    columns = COUM

    def rows(self):
        for row in pd.read_csv(file).values:
            yield tuple(row)

if __name__ == '__main__':
    luigi.build([ConDatabase(table=argv[1], file=argv[2])], local_scheduler=True)