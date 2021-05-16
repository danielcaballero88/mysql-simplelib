import logging
from mysql_simplelib.utils import mysql_methods

logger = logging.getLogger(__name__)

class Table:
    def __init__(self, db, name):
        self.db = db
        self.name = name
    
    # ---------------------------------------------
    # Insert

    def insert(self, dbConn, fields, records):
        logger.info('Trying to insert records into table "%s" in database "%s"', self.name, self.db.name)
        logger.debug('Records: %s', records)
        #
        if isinstance(records, tuple):
            self.__insert_one(dbConn, fields, records)
        elif isinstance(records, list):
            self.__insert_many(dbConn, fields, records)
        else:
            raise ValueError
        #

    def __insert_one(self, dbConn, fields, record, **kwargs):
        logger.debug('Inserting record using __insert_one() method')
        #
        query = 'INSERT INTO %s %s VALUES %s' % (self.name, fields, str(record))
        #
        self.db.execute(dbConn, query, after='commit', **kwargs)

    def __insert_many(self, dbConn, fields, records, **kwargs):
        logger.debug('Inserting records using __insert_many() method')
        #
        query = 'INSERT INTO %s %s VALUES (%%s, %%s)' % (self.name, fields)
        #
        self.db.execute(dbConn, query, many=True, params=records, after='commit', **kwargs)

    # ---------------------------------------------
    # Select

    def select(self, dbConn, limit=None, offset=None, where=None, **kwargs):
        logger.info('Trying to select records from table "%s" in database "%s"', self.name, self.db.name)
        # Assemble query
        query = f'SELECT * FROM {self.name}'
        if where:
            query += ' WHERE %s' % where
        if limit:
            query += ' LIMIT '
            if offset:
                query += '%d,' % offset
            query += '%d' % limit
        
        query += ';'
        logger.debug('Query: %s', query)
        #
        kwargs['after'] = kwargs.get('after', 'fetchall')
        result = self.db.execute(dbConn, query, **kwargs)
        return result