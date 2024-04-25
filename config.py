import os
import pathlib

from configparser import ConfigParser

ROOT = pathlib.Path(__file__).parent
DATA = pathlib.Path(ROOT, 'data', 'vacancies.json')
DATA_TEST = pathlib.Path(ROOT, 'data', 'test_file.json')
DATA_DATABASE = pathlib.Path(ROOT, 'database.ini')
PASS = os.getenv('password')


def config(filename=DATA_DATABASE, section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    if parser.has_section(section):
        params = parser.items(section)
        db = dict(params)
    else:
        raise Exception("Section {0} is not found in the {1} file".format(section, filename))
    return db
