import logging
from .utils import mysql_methods

logger = logging.getLogger(__name__)

class Server:
    def __init__(self, host):
        logger.info('New instance of Server: host=%ss', host)
        self.host = host

    # ---------------------------------------
    # Basic/Essential Methods: connect and execute

    def connect(self, user):
        logger.info('Connecting to server %s as user %s', self.host, user.name)
        connection = mysql_methods.connect_to_server(self.host, user.name, user.password)
        return connection

    def execute(self, connection, query, after=None, close='all'):
        logger.debug('Executing query at server connection level')
        # Execute given query
        result = mysql_methods.execute(connection, query, after, close)
        # Done
        return result

    # ---------------------------------------
    # Predefined methods for common operations

    def exists_database(self, connection, dbName):
        logger.info('Checking if db %s exists', dbName)
        query = (
            "SELECT count(*) "
            "FROM INFORMATION_SCHEMA.SCHEMATA "	
            f"WHERE SCHEMA_NAME = '{dbName}'"
        )
        # Execute query
        result = self.execute(connection, query, after='fetchone')
        dbExists = result[0]
        logger.debug('Database exists: %s', bool(dbExists))
        # Done
        return dbExists

    def create_database(self, connection, dbName, after=None, close='all'):
        logger.info('Trying to create Database of name: %s', dbName)
        query = f'CREATE DATABASE {dbName};'
        self.execute(connection, query, after, close)
    
    def drop_database(self, connection, dbName, ifExists=True, after=None, close='all'):
        logger.info('Droping Database %s', dbName)
        #
        if ifExists:
            query = 'DROP DATABASE IF EXISTS %s' % dbName
        else:
            query = 'DROP DATABASE %s' % dbName
        #
        self.execute(connection, query, after, close)