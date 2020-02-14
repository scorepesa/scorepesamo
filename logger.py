import logging
import sys

LOGCONFIG = {
   'version': 1,
   'disable_existing_loggers': False,
   'formatters': {
       'verbose': {
           'format': '%(levelname)s %(module)s P%(process)d \
           T%(thread)d %(message)s'
           },
       },
   'handlers': {
       'stdout': {
           'class': 'logging.StreamHandler',
           'stream': sys.stdout,
           'formatter': 'verbose',
           },
       'file_logger': {
           'class': 'logging.handlers.RotatingFileHandler',
           'filename': '/var/log/scorepesa/scorepesa_mo_consumer.log',
           'mode': 'a',
           'maxBytes': 1000,
           'backupCount': 10,
           'encoding': None,
           'delay': 0,
           'formatter': 'verbose',
           },
       'sys-logger6': {
           'class': 'logging.handlers.SysLogHandler',
           'address': '/dev/log',
           'facility': "local6",
           'formatter': 'verbose',
           },
       },
   'loggers': {
       'my-logger': {
           'handlers': ['sys-logger6', 'file_logger', 'stdout'],
           'level': logging.DEBUG,
           'propagate': True,
           },
       }
   }
