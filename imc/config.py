import os
import configparser

def get_config():
    """
    Get configuration
    """
    config = configparser.ConfigParser()

    if 'PROMINENCE_IMC_CONFIG_DIR' in os.environ:
        config.read('%s/imc.ini' % os.environ['PROMINENCE_IMC_CONFIG_DIR'])
    else:
        print('ERROR: Environment variable PROMINENCE_IMC_CONFIG_DIR has not been defined')
        exit(1)

    return config
