"""
Copyright 2019, Grid.

This source code is licensed under the ReNom Subscription Agreement, version 1.0.
ReNom Subscription Agreement Ver. 1.0 (https://www.renom.jp/info/license/index.html)
"""

from __future__ import print_function
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class Connector(with_metaclass(ABCMeta, object)):
    """
    Abstract class of db connector with sqlalchemy.
    """

    def __init__(self):
        self._initurl()
        self._initengine()
        self._initsession()

    @abstractmethod
    def _initurl(self):
        pass

    def _initengine(self):
        """
        Create local attribute of sqlalchemy.engine.Engine instance.
        """
        self._engine = create_engine(self.url, echo=False)

    @property
    def engine(self):
        """
        Returns:
            engine : sqlalchemy.engine.Engine
                return attribute of sqlalchemy.engine.Engine instance.
        """
        return self._engine

    def _initsession(self):
        """
        Create local attribute of sqlalchemy.orm.session.Session instance.
        """
        self._session = scoped_session(sessionmaker(bind=self._engine))

    @property
    def session(self):
        """
        Returns:
            session : sqlalchemy.orm.session.Session
                return attribute of sqlalchemy.orm.session.Session instance.
        """
        return self._session

    def get_all(self, obj):
        """
        Get all object.

        Args:
            obj : sqlalchemy.ext.declarative.api.DeclarativeMeta class object.

        Returns:
            obj_list : list of sqlalchemy.ext.declarative.api.DeclarativeMeta instance.
        """
        return self.session().query(obj).all()

    def insert(self, obj):
        """
        Insert object.

        Args:
            obj : instance of sqlalchemy.ext.declarative.api.DeclarativeMeta class object.

        Returns:
            obj : instance of inserted sqlalchemy.ext.declarative.api.DeclarativeMeta class object.
        """
        self.session().add(obj)
        self.session().commit()
        return obj

    def update(self, obj):
        """
        Update object. Same with insert function.

        Args:
            obj : instance of sqlalchemy.ext.declarative.api.DeclarativeMeta class object.

        Returns:
            obj : instance of updated sqlalchemy.ext.declarative.api.DeclarativeMeta class object.
        """
        self.insert(obj)


class SQLiteConnector(Connector):
    """
    Connector for SQLite.

    Examples:
        >>> from sqlalchemy import Column, Integer, TEXT
        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> from renom_cn.api.db import SQLiteConnector
        >>> Base = declarative_base()
        >>> class Test(Base):
        ...     __tablename__ = 'test'
        ...     id = Column(Integer, primary_key=True, autoincrement=True)
        ...     name = Column(TEXT)
        ...
        >>> conn = SQLiteConnector()
        >>> Base.metadata.create_all(bind=conn.engine)
        >>> data = Test(name="test1")
        >>> conn.insert(data)
        <__main__.Test object at 0x104bbbf28>
        >>> conn.get_all(Test)  # get all Test table object.
        [<__main__.Test object at 0x104bbbf28>]
        >>> conn.session.query(Test).one()  # get one with session attribute.
        <__main__.Test object at 0x104bbbf28>
        >>> conn.session.query(Test).filter(Test.name=="test1").one()  # select with filter.
        <__main__.Test object at 0x104bbbf28>
    """

    def __init__(self, host="", database=":memory:"):
        """
        initialize connection.

        Args:
            host : string, default "".
                hostname of DB server. default is localhost.

            database : string, default ":memory:".
                database name. default database is in memory.
        """
        self.host = host
        self.database = database
        super(SQLiteConnector, self).__init__()

    def _initurl(self):
        """
        initialize url.
        """
        self.url = "sqlite://{}/{}".format(self.host, self.database)


class MySQLConnector(Connector):
    """
    Connector for MySQL.

    Examples:
        >>> from sqlalchemy import Column, Integer, TEXT
        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> from renom_cn.api.db import SQLiteConnector
        >>> Base = declarative_base()
        >>> class Test(Base):
        ...     __tablename__ = 'test'
        ...     id = Column(Integer, primary_key=True, autoincrement=True)
        ...     name = Column(TEXT)
        ...
        >>> conn = MySQLConnector(host="localhost", port="3306",
        ...                       user="testuser", password="testpassword", database="testdb")
        >>> Base.metadata.create_all(bind=conn.engine)
        >>> data = Test(name="test1")
        >>> conn.insert(data)
        <__main__.Test object at 0x108a96550>
        >>> conn.get_all(Test)  # get all Test table object.
        [<__main__.Test object at 0x108a96550>]
        >>> conn.session.query(Test).one()  # get one with session attribute.
        <__main__.Test object at 0x108a96550>
        >>> conn.session.query(Test).filter(Test.name=="test1").one()  # select with filter.
        <__main__.Test object at 0x108a96550>
    """

    def __init__(self, host, port, user, password, database):
        """
        initialize connection.

        Args:
            host : string.
                hostname of DB server.

            port : string.
                port number.

            user : string.
                user name of database.

            password : string.
                password.

            database : string.
                database name.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(MySQLConnector, self).__init__()

    def _initurl(self):
        """
        initialize url.
        """
        self.url = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8".format(self.user,
                                                                        self.password,
                                                                        self.host,
                                                                        self.port,
                                                                        self.database)


class PostgreSQLConnector(Connector):
    """
    Connector for PostgreSQL.

    Examples:
        >>> from sqlalchemy import Column, Integer, TEXT
        >>> from sqlalchemy.ext.declarative import declarative_base
        >>> from renom_cn.api.db import PostgreSQLConnector
        >>> Base = declarative_base()
        >>> class Test(Base):
        ...     __tablename__ = 'test'
        ...     id = Column(Integer, primary_key=True, autoincrement=True)
        ...     name = Column(TEXT)
        ...
        >>> conn = PostgreSQLConnector(host="localhost", port="5432",
        ...                            user="testuser", password="testpassword", database="testdb")
        >>> Base.metadata.create_all(bind=conn.engine)
        >>> data = Test(name="test1")
        >>> conn.insert(data)
        <__main__.Test object at 0x10832e668>
        >>> conn.get_all(Test)  # get all Test table object.
        [<__main__.Test object at 0x10832e668>]
        >>> conn.session.query(Test).one()  # get one with session attribute.
        <__main__.Test object at 0x10832e668>
        >>> conn.session.query(Test).filter(Test.name=="test1").one()  # select with filter.
        <__main__.Test object at 0x10832e668>
    """
    def __init__(self, host, port, user, password, database):
        """
        initialize connection.

        Args:
            host : string.
                hostname of DB server.

            port : string.
                port number.

            user : string.
                user name of database.

            password : string.
                password.

            database : string.
                database name.
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(PostgreSQLConnector, self).__init__()

    def _initurl(self):
        """
        initialize url.
        """
        self.url = "postgresql://{}:{}@{}:{}/{}?charset=utf8".format(self.user,
                                                                     self.password,
                                                                     self.host,
                                                                     self.port,
                                                                     self.database)
