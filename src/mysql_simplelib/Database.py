import logging
from mysql_simplelib.utils import mysql_methods

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, server, name):
        logger.info('New instance of Database object %s', name)
        self.server = server
        self.name = name

    # ---------------------------------------------
    # Basic/Essential Methods: connect and execute

    def connect(self, user):
        logger.info('Connecting to database %s as user %s', self.name, user.name)
        connection = mysql_methods.connect_to_database(self.server.host, user.name, user.password, self.name)
        return connection

    def execute(self, connection, query, **kwargs):
        logger.debug('Database.execute() in db="%s"', self.name)
        logger.debug('Executing query: %s', query)
        # Execute given query
        result = mysql_methods.execute(connection, query, **kwargs)
        # Done
        return result

    # ---------------------------------------------
    # Predefined methods for common operations

    def exists_table(self, connection, tableName, close='cursor'):
        logger.info('Checking if table %s exists in database %s', tableName, self.name)
        query = (
            'SELECT count(*) '
            '  FROM INFORMATION_SCHEMA.TABLES '	
            '  WHERE TABLE_NAME = "%s"'
            '    AND TABLE_SCHEMA = "%s"'
        ) % (tableName, self.name)
        # Execute query
        result = self.execute(connection, query, after='fetchone', close=close)
        tableExists = result[0]
        logger.debug('Table exists: %s', bool(tableExists))
        # Done
        return tableExists

    def create_table(self, connection, tableName, fields, **kwargs):
        logger.info('Trying to create Table of name "%s" in Database "%s"', tableName, self.name)
        logger.debug('Fields: %s', fields)
        #
        fields = ', '.join('%s' % f for f in fields)
        query = 'CREATE TABLE %s (%s);' % (tableName, fields)
        self.execute(connection, query, **kwargs)
    
    def drop_table(self, connection, tableName, ifExists=True, **kwargs):
        logger.info('Droping Table "%s" from Database "%s"', tableName, self.name)
        #
        if ifExists:
            query = 'DROP TABLE IF EXISTS %s;' % tableName
        else:
            query = 'DROP TABLE %s;' % tableName
        #
        self.execute(connection, query, **kwargs)