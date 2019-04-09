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
        self.initurl()
        self.initengine()
        self.initsession()

    @abstractmethod
    def initurl(self):
        pass

    def initengine(self):
        """
        initialize engine.
        """
        self._engine = create_engine(self.url, echo=False)

    @property
    def engine(self):
        return self._engine

    def initsession(self):
        """
        initialize session.
        """
        self._session = scoped_session(sessionmaker(bind=self._engine))

    @property
    def session(self):
        return self._session

    def get_all(self, obj):
        """
        Get all object.

        Parameters
        ----------
            obj : Table class object.

        Returns
        -------
            result of .all() query.
        """
        return self.session().query(obj).all()

    def insert(self, obj):
        """
        Insert object.

        Parameters
        ----------
            obj : Table class object.

        Returns
        -------
            obj : inserted object.
        """
        self.session().add(obj)
        self.session().commit()
        return obj

    def update(self, obj):
        """
        Update object. Same with insert function.

        Parameters
        ----------
            obj : Table class object.

        Returns
        -------
            obj : inserted object.
        """
        self.insert(obj)


class SQLiteConnector(Connector):
    """
    Connector for SQLite.
    """

    def __init__(self, host="", database=":memory:"):
        """
        initialize.

        Parameters
        ----------
            host : string, default "".
                hostname of DB server.

            database : string, default ":memory:".
                database name.
        """
        self.host = host
        self.database = database
        super(SQLiteConnector, self).__init__()

    def initurl(self):
        """
        initialize url.
        """
        self.url = "sqlite://{}/{}".format(self.host, self.database)


class MySQLConnector(Connector):
    """
    Connector for MySQL.
    """

    def __init__(self, host, port, user, password, database):
        """
        initialize.

        Parameters
        ----------
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

    def initurl(self):
        """
        initialize url.
        """
        self.url = "mysql+pymysql://{}:{}@{}:{}/{}".format(self.user,
                                                           self.password,
                                                           self.host,
                                                           self.port,
                                                           self.database)


class PostgreSQLConnector(Connector):
    """
    Connector for PostgreSQL.
    """
    def __init__(self, host, port, user, password, database):
        """
        initialize.

        Parameters
        ----------
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

    def initurl(self):
        """
        initialize url.
        """
        self.url = "postgresql://{}:{}@{}:{}/{}".format(self.user,
                                                        self.password,
                                                        self.host,
                                                        self.port,
                                                        self.database)
