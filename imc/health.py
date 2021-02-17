from imc import config
from imc import database
from imc import imclient
from imc import im_utils

# Configuration
CONFIG = config.get_config()

def health():
    status = True
    msg = ''

    # Check Infrastructure Manager
    client = imclient.IMClient(url=CONFIG.get('im', 'url'))
    im_auth = im_utils.create_im_auth(None, None, None)
    (status, msg) = client.getauth(im_auth)
    if status != 0:
        return (False, 'Unable to create IM auth')

    (status, _) = client.list_infra_ids(10)
    if not status or status == 'timedout':
        return (False, 'Unable to connect to Infrastructure Manager')

    # Check database
    db = database.get_db()
    if db.connect():
        db.close()
    else:
        return (False, 'Unable to connect to database')

    return (status, msg)
