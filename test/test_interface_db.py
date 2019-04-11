from sqlalchemy import Column, Integer, TEXT
from sqlalchemy.ext.declarative import declarative_base

from renom_cn.api import db


Base = declarative_base()


class User(Base):
    """ test table """
    __tablename__ = 'user'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(TEXT)


def create_table(connection):
    Base.metadata.create_all(bind=connection.engine)


class TestSQLite(object):
    @classmethod
    def setup_class(self):
        self.test_data = {
            "id": 1,
            "name": "test"
        }
        self.conn = db.SQLiteConnector()
        create_table(self.conn)

    @classmethod
    def teardown_class(self):
        self.conn.session.close()

    def test_insert(self):
        d = User(name=self.test_data["name"])
        d = self.conn.insert(d)
        assert d.id == self.test_data["id"]

    def test_get_all(self):
        data_all = self.conn.get_all(User)
        for d in data_all:
            assert d.name == self.test_data["name"]

    def test_update(self):
        update_name = "test_update"
        data_all = self.conn.get_all(User)
        for d in data_all:
            d.name = update_name
            self.conn.update(d)

        data_all = self.conn.get_all(User)
        for d in data_all:
            assert d.name == update_name


# class TestMySQL(object):
#     @classmethod
#     def setup_class(self):
#         self.test_data = {
#             "id": 1,
#             "name": "test"
#         }
#         host = "localhost"
#         port = "3306"
#         user = "testuser"
#         password = "testpassword"
#         database = "testdb"
#         self.conn = db.MySQLConnector(host, port, user, password, database)
#         create_table(self.conn)
#
#     @classmethod
#     def teardown_class(self):
#         self.conn.session.close()
#
#     def test_insert(self):
#         d = User(name=self.test_data["name"])
#         d = self.conn.insert(d)
#         assert d.id == self.test_data["id"]
#
#     def test_get_all(self):
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             assert d.name == self.test_data["name"]
#
#     def test_update(self):
#         update_name = "test_update"
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             d.name = update_name
#             self.conn.update(d)
#
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             assert d.name == update_name
#
#
# class TestPostgreSQL(object):
#     @classmethod
#     def setup_class(self):
#         self.test_data = {
#             "id": 1,
#             "name": "test"
#         }
#         host = "localhost"
#         port = "5432"
#         user = "testuser"
#         password = "testpassword"
#         database = "testdb"
#         self.conn = db.PostgreSQLConnector(host, port, user, password, database)
#         create_table(self.conn)
#
#     @classmethod
#     def teardown_class(self):
#         self.conn.session.close()
#
#     def test_insert(self):
#         d = User(name=self.test_data["name"])
#         d = self.conn.insert(d)
#         assert d.id == self.test_data["id"]
#
#     def test_get_all(self):
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             assert d.name == self.test_data["name"]
#
#     def test_update(self):
#         update_name = "test_update"
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             d.name = update_name
#             self.conn.update(d)
#
#         data_all = self.conn.get_all(User)
#         for d in data_all:
#             assert d.name == update_name
