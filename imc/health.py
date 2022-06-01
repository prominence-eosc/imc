from imc import config
from imc import database

# Configuration
CONFIG = config.get_config()

def health():
    status = True
    msg = ''

    # Check database
    db = database.get_db()
    if db.connect():
        db.close()
    else:
        return (False, 'Unable to connect to database')

    return (status, msg)
