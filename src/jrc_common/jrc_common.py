''' Library of common routines. Very much a work in progress.
    Callable functions:
        call_arxiv
        call_biorxiv
        call_crossref
        call_datacite
        call_figshare
        call_protocolsio
        call_oa
        call_orcid
        call_people_by_id
        call_people_by_name
        call_people_by_suporg
        get_config
        get_pmid
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
from functools import wraps
import getpass
import json
import logging
from operator import attrgetter
import os
import smtplib
import time
from types import SimpleNamespace
import colorlog
import jwt
import requests
import xmltodict
try:
    import MySQLdb
except Exception:
    pass

# pylint: disable=logging-fstring-interpolation

# ****************************************************************************
# * Constants                                                                *
# ****************************************************************************
ARXIV_BASE = "https://export.arxiv.org/api/query?search_query="
BIORXIV_BASE = "https://api.biorxiv.org/details/biorxiv/"
CROSSREF_BASE = 'https://api.crossref.org/works/'
DATACITE_BASE = 'https://api.datacite.org/dois/'
FIGSHARE_BASE = 'https://api.figshare.com/v2/'
NCBI_BASE = 'https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/' \
            + '?tool=update_dois&email=svirskasr@hhmi.org&format=json&ids='
OA_BASE = 'https://bg.api.oa.works/report/works'
ORCID_BASE = 'https://pub.orcid.org/v3.0/'
OA_SUFFIX = '?q=(openalx.authorships.institutions.display_name:' \
            + 'janelia%20OR%20openalx.authorships.affiliations.raw_affiliation_string:' \
            + 'janelia%20OR%20openalx.authorships.institutions.ror:' \
            + '013sk6x84%20OR%20openalx.authorships.institutions.id:' \
            + '%22i195573530%22)%20AND%20((supplements.sheets:' \
            + '(%22pmc__hhmi%22%20OR%20%22name_epmc__hhmi%22%20OR%20%22' \
            + 'all-time__hhmi%22%20OR%20%22authorship__hhmi%22%20OR%20%22' \
            + 'staff__hhmi%22%20OR%20%22preprints_oa_locations__hhmi' \
            + '%22%20OR%20%22preprints-enrichment__hhmi%22)%20OR%20' \
            + '(funder.DOI:(%2210.13039/100000011%22)%20OR%20funder.name:' \
            + '(%22Howard%20Hughes%20Medical%20Institute%22%20OR' \
            + '%20%22Janelia%20Research%20Campus%22%20OR' \
            + '%20%22Freeman%20Hrabowski%22)%20OR%20openalx.grants.funder:' \
            + '(%22F4320306082%22))%20OR%20(authorships.institutions.ror:' \
            + '(%22006w34k90%22%20OR%20%22013sk6x84%22)%20OR' \
            + '%20authorships.institutions.display_name:' \
            + '(%22Howard%20Hughes%20Medical%20Institute%22%20OR%20%22' \
            + 'Janelia%20Research%20Campus%22%20OR' \
            + '%20%22Freeman%20Hrabowski%22)%20OR' \
            + '%20authorships.raw_affiliation_string:' \
            + '(%22Howard%20Hughes%20Medical%20Institute%22%20OR' \
            + '%20%22Janelia%20Research%20Campus%22%20OR' \
            + '%20%22Freeman%20Hrabowski%22))%20OR%20' \
            + 'supplements.funder.display_name_ic:%22hhmi%22)%20AND' \
            + '%20NOT%20(supplements.removed_from_report:' \
            + '%22hhmi%22%20OR%20supplements.is_financial_disclosure:' \
            + '%22hhmi%22))%20AND%20type:' \
            + '(%22article%22%20OR%20%22editorial%22%20OR' \
            + '%20%22letter%22%20OR%20%22review%22)%20AND%20NOT' \
            + '%20openalx.type_crossref:' \
            + '%22proceedings-article%22%20AND%20NOT' \
            + '%20(supplements.is_preprint:true%20OR' \
            + '%20(pubtype:preprint%20AND%20NOT%20supplements.is_preprint:' \
            + 'false)%20OR%20subtype:preprint)%20AND%20openalex:*%20AND' \
            + '%20journal:*'
PEOPLE_BASE = 'https://hhmipeople-prod.azurewebsites.net/People/'
PROTOCOLSIO_BASE = 'https://www.protocols.io/api/v3/'
TIMEOUT = (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
           requests.exceptions.Timeout)

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


def _call_url_old(url, headers=None, timeout=10):
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


def _call_url(url, headers=None, timeout=10, fmt='json'):
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
        if fmt == 'json':
            try:
                jstr = req.json()
            except Exception as err:
                raise requests.exceptions.JSONDecodeError("Could not decode response from " \
                                                          + f"{url} : {err}")
        elif fmt == 'xml':
            try:
                jstr = xmltodict.parse(req.text)
            except Exception as err:
                raise Exception("Could not decode XML response from " \
                                + f"{url} : {err}") from err
        else:
            raise Exception(f"Unknown format: {fmt}")
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
    from pymongo import MongoClient
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
    import psycopg2
    import psycopg2.extras
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
# * Decorators                                                               *
# ****************************************************************************
def retry(max_tries=3, delay=1, exceptions=TIMEOUT):
    """ Retry calling the decorated function using an exponential backoff.
        Keyword arguments:
          max_tries: Maximum number of times to try (default 3)
          delay: Initial delay between retries in seconds (default 1)
          exceptions: Exceptions to retry on (default RequestException)
        Returns:
          Response JSON or raised exception
    """
    def retry_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tries = 0
            while tries < max_tries:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    tries += 1
                    if tries == max_tries:
                        raise
                    wait_time = delay * (2 ** (tries - 1))
                    print(f"Retrying {func.__name__} requests.get in {wait_time:.2f} seconds... "
                          + f"(attempt {tries}/{max_tries})")
                    time.sleep(wait_time)
        return wrapper
    return retry_decorator


def wall_timer(msg=None, logger=None):
    ''' Decorator to time the execution of a function
        Keyword arguments:
          msg: message to print
        Returns:
          wrapped function
    '''
    def timer_decorator(func):
        @wraps(func)
        def wrapperx(*args, **kwargs):
            t = time.time()
            res = func(*args, **kwargs)
            elapsed_time = time.time() - t
            hh = int(elapsed_time // 3600)
            mm = int((elapsed_time % 3600) // 60)
            ss = elapsed_time % 60
            formatted_time = "{:02d}:{:02d}:{:05.2f}".format(hh, mm, ss)
            display_msg = msg if msg else func.__name__
            tlogger = logger if logger else logging.getLogger(__name__)
            tlogger.info(f"{display_msg} {formatted_time}")
            return res
        return wrapperx
    return timer_decorator

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
def send_email(mail_text, sender, receivers, subject, attachment=None, mime='plain', server=None):
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
    if server:
        mail_server = server
    else:
        try:
            servers = get_config("servers")
            mail_server = attrgetter("mail.address")(servers)
        except Exception as err:
            raise err
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

def call_arxiv(query, timeout=10):
    """ Get aRxiv data for a query
        Keyword arguments:
          query: query
          timeout: GET timeout
        Returns:
          Response XML or raised exception
    """
    try:
        response = _call_url(f"{ARXIV_BASE}{query}",
                             timeout=timeout, fmt='xml')
        return response
    except Exception as err:
        raise err


def call_biorxiv(doi, timeout=10):
    """ Get bioRxiv data for a DOI
        Keyword arguments:
          doi: DOI
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    try:
        response = _call_url(f"{BIORXIV_BASE}{doi}",
                             headers={"Accept": "application/json"},
                             timeout=timeout)
        return response
    except Exception as err:
        raise err


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


def call_figshare(doi, timeout=10):
    """ Get Figshare data for a DOI
        Keyword arguments:
          doi: DOI
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    try:
        response = _call_url(f"{FIGSHARE_BASE}articles?doi={doi}", timeout=timeout)
        return response
    except Exception as err:
        raise err


def call_oa(doi='', suffix='', timeout=10):
    """ Get OA data for a single DOI or for all Janelia OA works
        Keyword arguments:
          doi: DOI
          suffix: URL suffix
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{OA_BASE}/{doi}" if doi else f"{OA_BASE}{OA_SUFFIX}{suffix}"
    try:
        response = _call_url(url,
                             headers={"Accept": "application/json"},
                             timeout=timeout)
        return response
    except Exception as err:
        raise err


def call_orcid(oid, timeout=10):
    """ Get data from ORCID for a given ID
        Keyword arguments:
          oid: ORCID Id
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{ORCID_BASE}{oid}"
    try:
        response = _call_url(url,
                             headers={"Accept": "application/json"},
                             timeout=timeout)
        return response
    except Exception as err:
        raise err

@retry(max_tries=4, delay=2)
def call_people_by_id(eid, timeout=5):
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
    response = _call_url(url, headers=headers, timeout=timeout)
    if response and (('nameFirst' not in response) or (not response['nameFirst'])):
        return None
    return response


@retry(max_tries=4, delay=2)
def call_people_by_name(name, timeout=.001):
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
    response = _call_url(url, headers=headers, timeout=timeout)
    return response


@retry(max_tries=4, delay=2)
def call_people_by_suporg(code, page=0, timeout=10):
    """ Get suporg data from the People system by suporg code
        Keyword arguments:
          code: suporg code
          page: data page (starts at 0)
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{PEOPLE_BASE}GetByOrg/{code}/{page}"
    headers = {'APIKey': os.environ['PEOPLE_API_KEY'],
               'Content-Type': 'application/json'}
    response = _call_url(url, headers=headers, timeout=timeout)
    return response


def call_protocolsio(query, timeout=15):
    """ Get protocols.io data for a query
        Keyword arguments:
          query: query
          timeout: GET timeout
        Returns:
          Response JSON
    """
    headers = {'Authorization': f"Bearer {os.environ['PROTOCOLS_API_TOKEN']}"}
    try:
        response = _call_url(f"{PROTOCOLSIO_BASE}{query}",
                             headers=headers, timeout=timeout)
        return response
    except Exception as err:
        raise err


def get_pmid(doi, timeout=10):
    """ Convert a DOI to a PMID
        Keyword arguments:
          doi: DOI
          timeout: GET timeout
        Returns:
          Response JSON or raised exception
    """
    url = f"{NCBI_BASE}{doi}"
    try:
        response = _call_url(url, timeout=timeout)
    except Exception as err:
        raise err
    return response
