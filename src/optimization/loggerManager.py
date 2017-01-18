import logging
from logging.handlers import RotatingFileHandler

"""
This is the class for
Rotational Logger
"""
class LoggerManager(object):
    app_log = None

    def __init__(self, *args, **kwargs):
	    pass

    @staticmethod
    def getLogger():
        if LoggerManager.app_log is None:
            LOG_FORMAT = ("%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s" )
            log_formatter = logging.Formatter(LOG_FORMAT)
            # we need to change this path when the code is deployed 
            logFile = "/vagrant/fo_worker/logging/fot_rotate.log"  #Change this to server location
			#handler to change the log file after every 5MB
            my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024,backupCount=10, encoding=None, delay=0)
            my_handler.setFormatter(log_formatter)
			#Log level is set to INFO
            my_handler.setLevel(logging.INFO)
            LoggerManager.app_log = logging.getLogger('root')
            LoggerManager.app_log.setLevel(logging.INFO)
            LoggerManager.app_log.addHandler(my_handler)            
        return LoggerManager.app_log													

if __name__ == "__main__" :
    log=LoggerManager().getLogger()
    log.setLevel(level=logging.DEBUG)