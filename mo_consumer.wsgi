#!/usr/bin/python
import os
import sys
import site
import logging

#logging.basicConfig(stream=sys.stderr)

# Add the site-packages of the chosen virtualenv to work with
site.addsitedir('/apps/python/mov/lib/python2.7/site-packages')

# Add the app's directory to the PYTHONPATH
sys.path.append('/apps/python/scorepesa-mo')

# Activate your virtual env
activate_env=os.path.expanduser("/apps/python/mov/bin/activate_this.py")
execfile(activate_env, dict(__file__=activate_env))

from  application import app as application
#application.secret_key = 'Flask-py-susbscription-appp~#@)(*&!'

