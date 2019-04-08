from __future__ import print_function
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


class Connector(with_metaclass(ABCMeta, object)):
    """Abstract class of db connector with sqlalchemy."""

    def __init__(self):
        self.initurl()
        self.initengine()
        self.initsession()

    @abstractmethod
    def initurl(self):
        pass

    def initengine(self):
        self._engine = create_engine(self.url, echo=False)

    @property
    def engine(self):
        return self._engine

    def initsession(self):
        self._session = scoped_session(sessionmaker(bind=self._engine))

    @property
    def session(self):
        return self._session

    def get_all(self, obj):
        return self.session().query(obj).all()

    def insert(self, obj):
        self.session().add(obj)
        self.session().commit()
        return obj

    def update(self, obj):
        self.insert(obj)


class SQLiteConnector(Connector):
    def __init__(self, host="", database=":memory:"):
        self.is_type = "sqlite"
        self.host = host
        self.database = database
        super(SQLiteConnector, self).__init__()

    def initurl(self):
        self.url = "sqlite://{}/{}".format(self.host, self.database)


class MySQLConnector(Connector):
    def __init__(self, host, port, user, password, database):
        self.is_type = "mysql"
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(MySQLConnector, self).__init__()

    def initurl(self):
        self.url = "mysql+pymysql://{}:{}@{}:{}/{}".format(self.user,
                                                           self.password,
                                                           self.host,
                                                           self.port,
                                                           self.database)


class PostgreSQLConnector(Connector):
    def __init__(self, host, port, user, password, database):
        self.is_type = "postgresql"
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        super(PostgreSQLConnector, self).__init__()

    def initurl(self):
        self.url = "postgresql://{}:{}@{}:{}/{}".format(self.user,
                                                        self.password,
                                                        self.host,
                                                        self.port,
                                                        self.database)
