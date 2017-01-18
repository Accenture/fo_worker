import logging
from logging.handlers import RotatingFileHandler

class LoggerManager(object):
    """
    Created on Wed Jan 18 12:30:51 2017

    @author: omkar.marathe

    This is the class for
    Rotational Logger
    """

    app_log = None

    def __init__(self, *args, **kwargs):
	    pass

    @staticmethod
    def getLogger():
        if LoggerManager.app_log is None:
			LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s ')
			log_formatter = logging.Formatter(LOG_FORMAT)
			logFile = "C:\\Users\\omkar.marathe\\Desktop\\fot_rotate.log"  #Change this to server location
			#handler to change the log file after every 5MB 
			my_handler = RotatingFileHandler(logFile, mode='a', maxBytes=5*1024*1024,backupCount=10, encoding=None, delay=0)
			my_handler.setFormatter(log_formatter)
			#Log level is set to INFO
			my_handler.setLevel(logging.INFO)
			LoggerManager.app_log = logging.getLogger('root')
			LoggerManager.app_log.setLevel(logging.INFO)
			LoggerManager.app_log.addHandler(my_handler)
			print 'initializing log'
			
        return LoggerManager.app_log
			    
													

if __name__ == "__main__" :
    log=LoggerManager().getLogger()
    log.setLevel(level=logging.DEBUG)