from mysql_simplelib import User, Server, Database, Table
import logging
import os

if __name__ == '__main__':
    # Config logger
    format='%(asctime)s %(name)-12s %(funcName)-12s %(levelname)-8s %(message)s'
    logging.basicConfig(level='DEBUG', format=format)
    logger = logging.getLogger(__name__)
    try:
        # ---
        # USER
        here = os.path.dirname(__file__)
        dotenvPath = os.path.join(here, '.env')
        user = User.from_dotenv(dotenvPath)
        # ---
        # SERVER ACTIONS
        # Create Server object
        server = Server(host='localhost')
        # Create a database in the server
        if True:
            serverConn = server.connect(user)
            server.drop_database(serverConn, 'test1', close=None)
            server.create_database(serverConn, 'test1', close='all')
        # ---
        # DATABASE ACTIONS
        # Create a Database object
        db = Database(server=server, name='test1')
        dbConn = db.connect(user)
        #
        # Create a table in the database
        db.exists_table(dbConn, 'test1')
        db.drop_table(dbConn, 'test')
        #
        fields = [
            'id INT AUTO_INCREMENT PRIMARY KEY',
            'name VARCHAR(100) NOT NULL',
            'age DECIMAL(4,1)'
        ]
        db.create_table(dbConn, 'test', fields, close='all')
        # ---
        # TABLE ACTIONS
        # Create a Table object
        table = Table(db=db, name='test')
        # Insert some records
        dbConn = db.connect(user)
        singleRecord = ('Dani', 32)
        table.insert(dbConn, fields='(name, age)', records=singleRecord)
        manyRecords = [('Luli', 31), ('Oscar', 61), ('Mama', 65.5555)]
        table.insert(dbConn, fields='(name, age)', records=manyRecords)
        # Select records
        records = table.select(dbConn)
        logger.debug('Selected records: %s', records)
        records = table.select(dbConn, where='age<40')
        logger.debug('Selected records: %s', records)
        records = table.select(dbConn, limit=2)
        logger.debug('Selected records: %s', records)
        records = table.select(dbConn, limit=2, offset=1)
        logger.debug('Selected records: %s', records)
        records = table.select(dbConn, limit=2, offset=1, where='id>1')
        logger.debug('Selected records: %s', records)
    
    except Exception as e:
        logger.critical(e.__repr__())