''' Library of common routines. Very much a work in progress.
    Callable functions:
        get_config
        sql_error
        connect_database
        send_email
        check_token
        setup_logging
'''
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
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
# * Internal routines                                                        *
# ****************************************************************************
def _call_config_responder(endpoint):
    ''' Get a configuration from the configuration system
        Keyword arguments:
          endpoint: REST endpoint
        Returns:
          JSON response
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
            client = MongoClient(full_host, username=dbo.user,
                                 password=dbo.password, replicaSet=dbo.replicaset)
            connector = client[dbo.client]
        else:
            client = MongoClient(full_host, replicaSet=dbo.replicaset)
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
    elif dbo.type == "mysql":
        return _connect_mysql(dbo)
    elif dbo.type == "pg":
        return _connect_postgres(dbo)
    else:
        return None


# ****************************************************************************
# * Email                                                                    *
# ****************************************************************************
def send_email(mail_text, sender, receivers, subject, attachment=None):
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
    message.attach(MIMEText(mail_text, 'plain'))
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
