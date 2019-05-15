import logging

logger = logging.getLogger('') # <--- Probable a good idea to name your logger. '' is the 'root' logger
sysHandler = logging.StreamHandler()
sysHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(sysHandler)
logger.setLevel(logging.INFO)
