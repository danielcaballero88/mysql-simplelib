import logging
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

class User:
    def __init__(self, name, password):
        self.name = name
        self.password = password

    @classmethod
    def from_dotenv(cls, dotenvPath='.env'):
        load_dotenv(dotenv_path=dotenvPath)
        name = os.environ.get('DB_USER')
        password = os.environ.get('DB_PASSWORD')
        return cls(name, password)