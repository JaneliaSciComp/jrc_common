''' Library of common routines. Very much a work in progress.
    Callable functions:
        call_crossref
        call_datacite
        call_people_by_id
        call_people_by_name
        get_config
        get_run_data
        simplenamespace_to_dict
        sql_error
        connect_database
        send_email
        check_token
        setup_logging
'''

# pylint: disable=broad-exception-raised

from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import getpass
import json
from operator import attrgetter
import os
import smtplib
import time
from types import SimpleNamespace
import colorlog
import jwt
import MySQLdb
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
import requests


# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
CROSSREF_BASE = 'https://api.crossref.org/works/'
DATACITE_BASE = 'https://api.datacite.org/dois/'
PEOPLE_BASE = 'https://hhmipeople-prod.azurewebsites.net/People/'

# ****************************************************************************
# * Internal routines                                                        *
# ****************************************************************************
def _call_config_responder(endpoint):
    ''' Get a configuration from the configuration system
        Keyword arguments:
          endpoint: REST endpoint
        Returns:
          Response JSON or raised exception
    '''
    if not os.environ.get('CONFIG_SERVER_URL'):
        raise ValueError("Missing environment variable CONFIG_SERVER_URL")
    url = os.environ.get('CONFIG_SERVER_URL') + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        raise err
    if req.status_code == 200:
        try:
            jstr = req.json()
        except Exception as err:
            raise requests.exceptions.JSONDecodeError("Could not decode response from " \
                                                      + f"{url} : {err}")
        return jstr
    raise ConnectionError(f"Could not get response from {url}: {req.text}")


def _call_url(url, headers=None, timeout=10):
    ''' Get JSON from a URL (resumably a web API somewhere)
        Keyword arguments:
          url: URL
        Returns:
          JSON response
    '''
    try:
        if headers:
            req = requests.get(url, headers=headers, timeout=timeout)
        else:
            req = requests.get(url, timeout=timeout)
    except requests.exceptions.RequestException as err:
        raise err
    if req.status_code == 200:
        try:
            jstr = req.json()
        except Exception as err:
            raise requests.exceptions.JSONDecodeError("Could not decode response from " \
                                                      + f"{url} : {err}")
        return jstr
    if req.status_code == 404:
        return {}
    raise Exception(f"Status: {str(req.status_code)} ({url})")


def _connect_mongo(dbo):
    """ Connect to a MongoDB database. If a port is not specified,
        the default is used.
        Keyword arguments:
          dbo: database namespace object
        Returns:
          connector
    """
    full_host = f"{dbo.host}:" \
                + ({dbo.port} if hasattr(dbo, "port") and dbo.port else "27017")
    try:
        if hasattr(dbo, "password") and dbo.password:
            if hasattr(dbo, "replicaset") and dbo.replicaset:
                client = MongoClient(full_host, username=dbo.user,
                                     password=dbo.password, replicaSet=dbo.replicaset)
            else:
                client = MongoClient(full_host, username=dbo.user,
                                     password=dbo.password)
            connector = client[dbo.client]
        else:
            if hasattr(dbo, "replicaset") and dbo.replicaset:
                client = MongoClient(full_host, replicaSet=dbo.replicaset)
            else:
                client = MongoClient(full_host)
            connector = client[dbo.client]
    except Exception as err:
        raise err
    return connector


def _connect_mysql(dbo):
    """ Connect to a MySQL database. If a port is not specified,
        the default is used.
        Keyword arguments:
          dbo: database namespace object
        Returns:
          cursor
    """
    port = dbo.port if hasattr(dbo, "port") and dbo.port else 3306
    try:
        conn = MySQLdb.connect(host=dbo.host, port=port, user=dbo.user,
                               passwd=dbo.password, db=dbo.name)
    except MySQLdb.Error as err:
        raise MySQLdb.Error(err)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        return {"conn": conn, "cursor": cursor}
    except MySQLdb.Error as err:
        raise MySQLdb.Error(err)


def _connect_postgres(dbo):
    """ Connect to a Postgres database.
        Keyword arguments:
          dbo: database namespace object
        Returns:
          cursor
    """
    try:
        conn = psycopg2.connect(host=dbo.host, database=dbo.name,user=dbo.user)
        cursor = conn.cursor()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    except Exception as err:
        raise err
    return {"conn": conn, "cursor": cursor}


def _decode_token(token):
    ''' Decode a given JWT token (no signature)
        Keyword arguments:
          token: JWT token
        Returns:
          decoded token JSON or string error
    '''
    try:
        response = jwt.decode(token, options={"verify_signature": False})
        response = jwt.api_jwt.decode_complete(token, options={"verify_signature": False})
    except jwt.exceptions.DecodeError:
        return "JSON Web Token failed validation"
    except jwt.exceptions.InvalidTokenError:
        return "Could not decode JSON Web Token"
    if int(time.time()) >= response['payload']['exp']:
        return "Your JSON Web Token is expired"
    return response


# ****************************************************************************
# * Configuration                                                            *
# ****************************************************************************
def get_config(config):
    """ Convert the JSON received from a configuration to an object
        Keyword arguments:
          config: configuration name
        Returns:
          Configuration namespace object
    """
    try:
        data = (_call_config_responder(f"config/{config}"))["config"]
    except Exception as err:
        raise err
    return json.loads(json.dumps(data), object_hook=lambda dat: SimpleNamespace(**dat))


def get_user_name():
    """ Return the name of the user running the program
        Keyword arguments:
          None
        Returns:
          User name
    """
    user = getpass.getuser()
    if user:
        try:
            workday = simplenamespace_to_dict(get_config("workday"))
        except Exception as err:
            raise err
        if user in workday:
            rec = workday[user]
            return f"{rec['first']} {rec['last']}"
        return user
    return None


def get_run_data(program, version):
    """ Get a run data message with program name/version, user, and date/time
        Keyword arguments:
          program: program name
          version: program version
        Returns:
          Run data message
    """
    msg = f"{os.path.basename(program)} (version {version})"
    try:
        uname = get_user_name()
    except Exception as err:
        raise err
    if uname:
        msg += f" run by {uname} at {datetime.now()}\n"
    else:
        msg += f" run at {datetime.now()}\n"
    return msg


def simplenamespace_to_dict(nspace):
    """ Convert a simplenamespace to a dict recursively
        Keyword arguments:
          nspace: simplenamespace to convert
        Returns:
          The converted dict
    """
    result = {}
    for key, value in nspace.__dict__.items():
        if isinstance(value, SimpleNamespace):
            result[key] = simplenamespace_to_dict(value)
        else:
            result[key] = value
    return result


# ****************************************************************************
# * Database                                                                 *
# ****************************************************************************
def sql_error(err):
    """ Log a critical SQL error and exit
        Keyword arguments:
          err: error object
    """
    try:
        return f"MySQL error [{err.args[0]}]: {err.args[1]}"
    except IndexError:
        return f"MySQL error {str(err)}"


def connect_database(dbo):
    """ Convenience function to connect to a database
        Keyword arguments:
          dbo: database namespace object
        Returns:
          return from called function
    """
    if dbo.type == "mongo":
        return _connect_mongo(dbo)
    if dbo.type == "mysql":
        return _connect_mysql(dbo)
    if dbo.type == "pg":
        return _connect_postgres(dbo)
    return None


# ****************************************************************************
# * Email                                                                    *
# ****************************************************************************
def send_email(mail_text, sender, receivers, subject, attachment=None, mime='plain'):
    """ Send an email
        Keyword arguments:
          mail_text: body of email message
          sender: sender address
          receivers: list of recipients
          subject: email subject
          attachment: attachment file name
        Returns:
          None
    """
    try:
        servers = get_config("servers")
    except Exception as err:
        raise err
    mail_server = attrgetter("mail.address")(servers)
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ", ".join(receivers)
    message["Subject"] =subject
    message.attach(MIMEText(mail_text, mime))
    if attachment:
        attach_file_name = attachment
        with open(attach_file_name, 'rb') as attach_file: # open the file as binary mode
            payload = MIMEBase('application', 'octate-stream')
            payload.set_payload((attach_file).read())
        encoders.encode_base64(payload) # encode the attachment
        # Add payload header with filename
        payload.add_header('Content-Decomposition', 'attachment', filename=attach_file_name)
        message.attach(payload)
    try:
        smtpobj = smtplib.SMTP(mail_server)
        smtpobj.sendmail(sender, receivers, message.as_string())
        smtpobj.quit()
    except smtplib.SMTPException as err:
        raise smtplib.SMTPException("There was a error and the email was not sent:\n" + err)
    except Exception as err:
        raise err

# ****************************************************************************
# * JWT                                                                      *
# ****************************************************************************
def check_token(env='JACS_JWT'):
    """ Check a JSON Web Token
        Keyword arguments:
          env: environment variable containing token
        Returns:
          decoded token JSON or string error
    """
    if env not in os.environ:
        return f"Missing JSON Web Token - set in {env} environment variable"
    return _decode_token(os.environ[env])


# ****************************************************************************
# * Logging                                                                  *
# ****************************************************************************
def setup_logging(arg):
    """ Set up colorlog logging
        Keyword arguments:
          arg: argparse arguments
        Returns:
          colorlog handler
    """
    logger = colorlog.getLogger()
    if arg.DEBUG:
        logger.setLevel(colorlog.DEBUG)
    elif arg.VERBOSE:
        logger.setLevel(colorlog.INFO)
    else:
        logger.setLevel(colorlog.WARNING)
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(handler)
    return logger


# ****************************************************************************
# * REST                                                                     *
# ****************************************************************************
def call_crossref(doi, timeout=10):
    """ Get Crossref data for a DOI
        Keyword arguments:
          doi: DOI
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    try:
        response = _call_url(f"{CROSSREF_BASE}{doi}",
                             headers={'mailto': 'svirskasr@hhmi.org'},
                             timeout=timeout)
        return response
    except Exception as err:
        raise err


def call_datacite(doi, timeout=10):
    """ Get DataCite data for a DOI
        Keyword arguments:
          doi: DOI
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    try:
        response = _call_url(f"{DATACITE_BASE}{doi}", timeout=timeout)
        return response
    except Exception as err:
        raise err


def call_people_by_id(eid, timeout=10):
    """ Get person data from the People system by employee ID
        Keyword arguments:
          eid: employee ID
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{PEOPLE_BASE}Person/GetById/{eid}"
    headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
               'Content-Type': 'application/json'}
    try:
        response = _call_url(url, headers=headers, timeout=timeout)
    except Exception as err:
        raise err
    return response


def call_people_by_name(name, timeout=10):
    """ Get person data from the People system by name
        Keyword arguments:
          name: name
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{PEOPLE_BASE}Search/ByName/{name}"
    headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
               'Content-Type': 'application/json'}
    try:
        response = _call_url(url, headers=headers, timeout=timeout)
    except Exception as err:
        raise err
    return response
