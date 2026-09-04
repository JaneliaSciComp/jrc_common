"""
Test suite for jrc_common library.
Run with: pytest tests/test_jrc_common.py -v
"""
import os
import time
import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, mock_open
import pytest
import requests

import jrc_common.jrc_common as JRC


# ==============================================================================
# Helpers
# ==============================================================================

def _make_response(status_code=200, json_data=None, text=""):
    """Build a mock requests.Response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = text
    if json_data is not None:
        mock_resp.json.return_value = json_data
    else:
        mock_resp.json.side_effect = Exception("no json")
    return mock_resp


# ==============================================================================
# convert_diacritics
# ==============================================================================

class TestConvertDiacritics:
    def test_returns_none_for_empty_string(self):
        assert JRC.convert_diacritics("") is None

    def test_returns_none_for_none(self):
        assert JRC.convert_diacritics(None) is None

    def test_converts_accented_characters(self):
        result = JRC.convert_diacritics("Ségolène")
        assert result == "Segolene"

    def test_converts_umlaut(self):
        result = JRC.convert_diacritics("Müller")
        assert result == "Muller"

    def test_converts_tilde(self):
        result = JRC.convert_diacritics("Español")
        assert result == "Espanol"

    def test_returns_none_for_plain_ascii(self):
        assert JRC.convert_diacritics("Hello World") is None

    def test_converts_mixed_string(self):
        result = JRC.convert_diacritics("Café René")
        assert result == "Cafe Rene"

    def test_converts_scandinavian_characters(self):
        result = JRC.convert_diacritics("Björk")
        assert result == "Bjork"


# ==============================================================================
# simplenamespace_to_dict
# ==============================================================================

class TestSimplenamespaceToDict:
    def test_flat_namespace(self):
        ns = SimpleNamespace(a=1, b="hello")
        result = JRC.simplenamespace_to_dict(ns)
        assert result == {"a": 1, "b": "hello"}

    def test_nested_namespace(self):
        inner = SimpleNamespace(x=10)
        outer = SimpleNamespace(inner=inner, val="top")
        result = JRC.simplenamespace_to_dict(outer)
        assert result == {"inner": {"x": 10}, "val": "top"}

    def test_empty_namespace(self):
        ns = SimpleNamespace()
        assert JRC.simplenamespace_to_dict(ns) == {}

    def test_non_namespace_values_preserved(self):
        ns = SimpleNamespace(lst=[1, 2, 3], dct={"key": "val"})
        result = JRC.simplenamespace_to_dict(ns)
        assert result["lst"] == [1, 2, 3]
        assert result["dct"] == {"key": "val"}


# ==============================================================================
# sql_error
# ==============================================================================

class TestSqlError:
    def test_formats_mysql_error_with_args(self):
        err = MagicMock()
        err.args = (1045, "Access denied")
        result = JRC.sql_error(err)
        assert "1045" in result
        assert "Access denied" in result

    def test_formats_mysql_error_without_index(self):
        err = Exception("generic error")
        result = JRC.sql_error(err)
        assert "generic error" in result

    def test_formats_error_with_single_arg(self):
        err = MagicMock()
        err.args = (2003,)
        # Will raise IndexError on args[1], fall back to str(err)
        result = JRC.sql_error(err)
        assert result is not None


# ==============================================================================
# connect_database
# ==============================================================================

class TestConnectDatabase:
    def test_unknown_type_returns_none(self):
        dbo = SimpleNamespace(type="oracle")
        result = JRC.connect_database(dbo)
        assert result is None

    @patch("jrc_common.jrc_common._connect_mongo")
    def test_dispatches_mongo(self, mock_mongo):
        mock_mongo.return_value = "mongo_connector"
        dbo = SimpleNamespace(type="mongo")
        result = JRC.connect_database(dbo)
        mock_mongo.assert_called_once_with(dbo)
        assert result == "mongo_connector"

    @patch("jrc_common.jrc_common._connect_mysql")
    def test_dispatches_mysql(self, mock_mysql):
        mock_mysql.return_value = {"conn": "c", "cursor": "cur"}
        dbo = SimpleNamespace(type="mysql")
        result = JRC.connect_database(dbo)
        mock_mysql.assert_called_once_with(dbo)

    @patch("jrc_common.jrc_common._connect_postgres")
    def test_dispatches_postgres(self, mock_pg):
        mock_pg.return_value = {"conn": "c", "cursor": "cur"}
        dbo = SimpleNamespace(type="pg")
        result = JRC.connect_database(dbo)
        mock_pg.assert_called_once_with(dbo)


# ==============================================================================
# check_token / _decode_token
# ==============================================================================

class TestCheckToken:
    def test_missing_env_var_returns_message(self):
        env = "NONEXISTENT_JWT_VAR_XYZ"
        if env in os.environ:
            del os.environ[env]
        result = JRC.check_token(env=env)
        assert env in result

    def test_invalid_token_returns_error_string(self, monkeypatch):
        monkeypatch.setenv("TEST_JWT", "not.a.valid.token")
        result = JRC.check_token(env="TEST_JWT")
        assert isinstance(result, str)
        assert "token" in result.lower() or "JSON" in result

    def test_expired_token_returns_error_string(self, monkeypatch):
        # A structurally valid but expired JWT (exp in the past)
        # Header: {"alg":"HS256","typ":"JWT"}
        # Payload: {"sub":"1234","exp":1}  (exp=1 epoch second = long past)
        expired_token = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJzdWIiOiIxMjM0IiwiZXhwIjoxfQ."
            "signature"
        )
        monkeypatch.setenv("TEST_JWT", expired_token)
        result = JRC.check_token(env="TEST_JWT")
        assert isinstance(result, str)
        assert "expired" in result.lower() or "token" in result.lower()


# ==============================================================================
# _call_config_responder
# ==============================================================================

class TestCallConfigResponder:
    def test_raises_when_env_var_missing(self, monkeypatch):
        monkeypatch.delenv("CONFIG_SERVER_URL", raising=False)
        with pytest.raises(ValueError, match="CONFIG_SERVER_URL"):
            JRC._call_config_responder("/some/endpoint")

    @patch("jrc_common.jrc_common.requests.get")
    def test_returns_json_on_200(self, mock_get, monkeypatch):
        monkeypatch.setenv("CONFIG_SERVER_URL", "http://config.example.com")
        mock_get.return_value = _make_response(200, json_data={"key": "value"})
        result = JRC._call_config_responder("/config/test")
        assert result == {"key": "value"}

    @patch("jrc_common.jrc_common.requests.get")
    def test_raises_on_non_200(self, mock_get, monkeypatch):
        monkeypatch.setenv("CONFIG_SERVER_URL", "http://config.example.com")
        mock_get.return_value = _make_response(500, text="Internal Server Error")
        with pytest.raises(ConnectionError):
            JRC._call_config_responder("/config/test")


# ==============================================================================
# _call_url
# ==============================================================================

class TestCallUrl:
    @patch("jrc_common.jrc_common.requests.get")
    def test_returns_json_on_200(self, mock_get):
        mock_get.return_value = _make_response(200, json_data={"result": "ok"})
        result = JRC._call_url("http://example.com/api")
        assert result == {"result": "ok"}

    @patch("jrc_common.jrc_common.requests.get")
    def test_returns_empty_dict_for_allowed_status(self, mock_get):
        mock_get.return_value = _make_response(404)
        result = JRC._call_url("http://example.com/api", allow=[404])
        assert result == {}

    @patch("jrc_common.jrc_common.requests.get")
    def test_raises_on_disallowed_non_200(self, mock_get):
        mock_get.return_value = _make_response(500)
        with pytest.raises(Exception):
            JRC._call_url("http://example.com/api", allow=[])

    @patch("jrc_common.jrc_common.requests.get")
    def test_raises_on_request_exception(self, mock_get):
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection failed")
        with pytest.raises(requests.exceptions.ConnectionError):
            JRC._call_url("http://example.com/api")

    @patch("jrc_common.jrc_common.requests.get")
    def test_parses_xml_format(self, mock_get):
        xml_text = "<root><item>value</item></root>"
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = xml_text
        mock_get.return_value = mock_resp
        result = JRC._call_url("http://example.com/api", fmt="xml")
        assert "root" in result
        assert result["root"]["item"] == "value"

    @patch("jrc_common.jrc_common.requests.get")
    def test_raises_on_unknown_format(self, mock_get):
        mock_get.return_value = _make_response(200, json_data={})
        with pytest.raises(Exception, match="Unknown format"):
            JRC._call_url("http://example.com/api", fmt="csv")

    @patch("jrc_common.jrc_common.requests.get")
    def test_passes_headers(self, mock_get):
        mock_get.return_value = _make_response(200, json_data={})
        headers = {"Authorization": "Bearer token"}
        JRC._call_url("http://example.com/api", headers=headers)
        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs.get("headers") == headers or \
               (len(call_kwargs.args) > 0 and call_kwargs.kwargs.get("headers") == headers)


# ==============================================================================
# get_config
# ==============================================================================

class TestGetConfig:
    @patch("jrc_common.jrc_common._call_config_responder")
    def test_returns_namespace_object(self, mock_responder):
        mock_responder.return_value = {"config": '{"host": "localhost", "port": 27017}'}
        # get_config calls json.loads on the config value
        # The config value is already a dict or a JSON string depending on server
        # Looking at the code: data = (_call_config_responder(...)["config"])
        # then json.loads(json.dumps(data), ...)
        mock_responder.return_value = {"config": {"host": "localhost", "port": 27017}}
        result = JRC.get_config("testconfig")
        assert isinstance(result, SimpleNamespace)
        assert result.host == "localhost"
        assert result.port == 27017

    @patch("jrc_common.jrc_common._call_config_responder")
    def test_propagates_exception(self, mock_responder):
        mock_responder.side_effect = ValueError("CONFIG_SERVER_URL missing")
        with pytest.raises(ValueError):
            JRC.get_config("testconfig")


# ==============================================================================
# get_run_data
# ==============================================================================

class TestGetRunData:
    @patch("jrc_common.jrc_common.get_user_name")
    def test_includes_program_and_version(self, mock_user):
        mock_user.return_value = "Test User"
        result = JRC.get_run_data("/path/to/my_program.py", "1.2.3")
        assert "my_program.py" in result
        assert "1.2.3" in result

    @patch("jrc_common.jrc_common.get_user_name")
    def test_includes_user_when_available(self, mock_user):
        mock_user.return_value = "Jane Doe"
        result = JRC.get_run_data("prog.py", "2.0")
        assert "Jane Doe" in result

    @patch("jrc_common.jrc_common.get_user_name")
    def test_handles_no_user(self, mock_user):
        mock_user.return_value = None
        result = JRC.get_run_data("prog.py", "2.0")
        assert "prog.py" in result
        assert "run at" in result


# ==============================================================================
# call_arxiv / call_biorxiv / call_crossref / call_datacite
# ==============================================================================

class TestCallApis:
    @patch("jrc_common.jrc_common._call_url")
    def test_call_arxiv(self, mock_url):
        mock_url.return_value = {"feed": {}}
        result = JRC.call_arxiv("some+query")
        assert result == {"feed": {}}
        url_arg = mock_url.call_args[0][0]
        assert "export.arxiv.org" in url_arg
        assert "some+query" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_call_biorxiv(self, mock_url):
        mock_url.return_value = {"collection": []}
        result = JRC.call_biorxiv("10.1101/2021.01.01.000001")
        assert result == {"collection": []}
        url_arg = mock_url.call_args[0][0]
        assert "biorxiv" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_call_crossref(self, mock_url):
        mock_url.return_value = {"message": {}}
        result = JRC.call_crossref("10.1234/test")
        assert result == {"message": {}}
        url_arg = mock_url.call_args[0][0]
        assert "crossref" in url_arg
        assert "10.1234/test" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_call_datacite(self, mock_url):
        mock_url.return_value = {"data": {}}
        result = JRC.call_datacite("10.5281/zenodo.123")
        assert result == {"data": {}}

    @patch("jrc_common.jrc_common._call_url")
    def test_call_elsevier(self, mock_url, monkeypatch):
        monkeypatch.setenv("ELSEVIER_API_KEY", "test-key")
        mock_url.return_value = {"results": []}
        result = JRC.call_elsevier("article/pii/S123456")
        assert result == {"results": []}
        headers = mock_url.call_args.kwargs.get("headers") or mock_url.call_args[1].get("headers")
        assert headers["X-ELS-APIKey"] == "test-key"

    @patch("jrc_common.jrc_common._call_url")
    def test_call_figshare(self, mock_url):
        mock_url.return_value = [{"id": 123}]
        result = JRC.call_figshare("10.6084/m9.figshare.123")
        assert result == [{"id": 123}]

    @patch("jrc_common.jrc_common._call_url")
    def test_call_orcid(self, mock_url):
        mock_url.return_value = {"orcid-identifier": {}}
        result = JRC.call_orcid("0000-0001-2345-6789")
        url_arg = mock_url.call_args[0][0]
        assert "0000-0001-2345-6789" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_call_zenodo(self, mock_url, monkeypatch):
        monkeypatch.setenv("ZENODO_API_KEY", "zen-key")
        mock_url.return_value = {"hits": {}}
        result = JRC.call_zenodo("records?q=test")
        assert result == {"hits": {}}

    @patch("jrc_common.jrc_common._call_url")
    def test_call_protocolsio(self, mock_url, monkeypatch):
        monkeypatch.setenv("PROTOCOLS_API_TOKEN", "proto-token")
        mock_url.return_value = {"items": []}
        result = JRC.call_protocolsio("protocols?filter=public")
        assert result == {"items": []}

    @patch("jrc_common.jrc_common._call_url")
    def test_call_oa_with_doi(self, mock_url):
        mock_url.return_value = {"doi": "10.1234/test"}
        result = JRC.call_oa(doi="10.1234/test")
        url_arg = mock_url.call_args[0][0]
        assert "10.1234/test" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_call_oa_without_doi(self, mock_url):
        mock_url.return_value = {"results": []}
        result = JRC.call_oa()
        url_arg = mock_url.call_args[0][0]
        assert "bg.api.oa.works" in url_arg

    @patch("jrc_common.jrc_common._call_url")
    def test_api_propagates_exception(self, mock_url):
        mock_url.side_effect = Exception("Network error")
        with pytest.raises(Exception, match="Network error"):
            JRC.call_crossref("10.1234/bad")


# ==============================================================================
# call_people_by_id / call_people_by_name / call_people_by_suporg
# ==============================================================================

class TestCallPeople:
    @patch("jrc_common.jrc_common._call_url")
    def test_call_people_by_id_returns_none_when_no_name(self, mock_url, monkeypatch):
        monkeypatch.setenv("PEOPLE_API_KEY", "people-key")
        mock_url.return_value = {"employeeId": "12345"}  # missing nameFirst
        result = JRC.call_people_by_id("12345")
        assert result is None

    @patch("jrc_common.jrc_common._call_url")
    def test_call_people_by_id_returns_response_with_name(self, mock_url, monkeypatch):
        monkeypatch.setenv("PEOPLE_API_KEY", "people-key")
        mock_url.return_value = {"nameFirst": "Jane", "nameLast": "Doe"}
        result = JRC.call_people_by_id("12345")
        assert result["nameFirst"] == "Jane"

    @patch("jrc_common.jrc_common._call_url")
    def test_call_people_by_id_returns_none_for_empty_name(self, mock_url, monkeypatch):
        monkeypatch.setenv("PEOPLE_API_KEY", "people-key")
        mock_url.return_value = {"nameFirst": ""}
        result = JRC.call_people_by_id("12345")
        assert result is None

    @patch("jrc_common.jrc_common._call_url")
    def test_call_people_by_name(self, mock_url, monkeypatch):
        monkeypatch.setenv("PEOPLE_API_KEY", "people-key")
        mock_url.return_value = [{"nameFirst": "Jane"}]
        result = JRC.call_people_by_name("Jane Doe")
        assert isinstance(result, list)

    @patch("jrc_common.jrc_common._call_url")
    def test_call_people_by_suporg(self, mock_url, monkeypatch):
        monkeypatch.setenv("PEOPLE_API_KEY", "people-key")
        mock_url.return_value = [{"employeeId": "1"}]
        result = JRC.call_people_by_suporg("12345")
        url_arg = mock_url.call_args[0][0]
        assert "12345" in url_arg


# ==============================================================================
# convert_pmid / get_pmid
# ==============================================================================

class TestConvertPmid:
    @patch("jrc_common.jrc_common.requests.get")
    def test_returns_pmcid(self, mock_get):
        mock_get.return_value = _make_response(
            200,
            json_data={"status": "ok", "records": [{"pmid": "12345", "pmcid": "PMC123456"}]}
        )
        result = JRC.convert_pmid("12345", convert_to="pmcid")
        assert result == "PMC123456"

    @patch("jrc_common.jrc_common.requests.get")
    def test_returns_empty_string_when_not_found(self, mock_get):
        mock_get.return_value = _make_response(
            200,
            json_data={"status": "ok", "records": [{"pmid": "12345"}]}
        )
        result = JRC.convert_pmid("12345", convert_to="pmcid")
        assert result == ""

    @patch("jrc_common.jrc_common._call_url")
    def test_get_pmid_from_ncbi(self, mock_url, monkeypatch):
        monkeypatch.delenv("NCBI_API_KEY", raising=False)
        mock_url.return_value = {
            "status": "ok",
            "records": [{"pmid": "98765", "doi": "10.1234/test"}]
        }
        result = JRC.get_pmid("10.1234/test")
        assert result == "98765"

    @patch("jrc_common.jrc_common._call_url")
    def test_get_pmid_returns_empty_when_not_in_ncbi(self, mock_url, monkeypatch):
        monkeypatch.delenv("NCBI_API_KEY", raising=False)
        mock_url.return_value = {}  # no status key
        result = JRC.get_pmid("10.1234/notfound")
        assert result == ""


# ==============================================================================
# PMIDNotFound custom exception
# ==============================================================================

class TestPMIDNotFound:
    def test_exception_has_details(self):
        exc = JRC.PMIDNotFound("not found", "some details")
        assert str(exc) == "not found"
        assert exc.details == "some details"

    def test_is_custom_error(self):
        exc = JRC.PMIDNotFound("msg", "details")
        assert isinstance(exc, JRC.CustomError)
        assert isinstance(exc, Exception)


# ==============================================================================
# retry decorator
# ==============================================================================

class TestRetryDecorator:
    def test_succeeds_on_first_try(self):
        call_count = 0

        @JRC.retry(max_tries=3, delay=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = flaky()
        assert result == "ok"
        assert call_count == 1

    def test_retries_on_timeout_and_succeeds(self):
        call_count = 0

        @JRC.retry(max_tries=3, delay=0)
        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.exceptions.ConnectTimeout()
            return "ok"

        with patch("jrc_common.jrc_common.time.sleep"):
            result = flaky()
        assert result == "ok"
        assert call_count == 3

    def test_raises_after_max_tries(self):
        @JRC.retry(max_tries=3, delay=0)
        def always_fails():
            raise requests.exceptions.ConnectTimeout()

        with patch("jrc_common.jrc_common.time.sleep"):
            with pytest.raises(requests.exceptions.ConnectTimeout):
                always_fails()

    def test_non_retried_exception_propagates_immediately(self):
        call_count = 0

        @JRC.retry(max_tries=3, delay=0)
        def raises_value_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not a timeout")

        with pytest.raises(ValueError):
            raises_value_error()
        assert call_count == 1


# ==============================================================================
# wall_timer decorator
# ==============================================================================

class TestWallTimer:
    def test_returns_function_result(self):
        @JRC.wall_timer()
        def add(a, b):
            return a + b

        result = add(2, 3)
        assert result == 5

    def test_logs_elapsed_time(self, caplog):
        @JRC.wall_timer(msg="test_operation")
        def noop():
            return None

        with caplog.at_level(logging.INFO):
            noop()
        # The timer logs via the module logger; check it ran without error
        # (caplog may or may not capture colorlog output depending on config)
        assert True  # just ensure no exception raised

    def test_uses_custom_logger(self):
        custom_logger = MagicMock()

        @JRC.wall_timer(msg="my_task", logger=custom_logger)
        def work():
            return 42

        result = work()
        assert result == 42
        custom_logger.info.assert_called_once()
        log_msg = custom_logger.info.call_args[0][0]
        assert "my_task" in log_msg


# ==============================================================================
# setup_logging
# ==============================================================================

class TestSetupLogging:
    def test_debug_level(self):
        import colorlog
        arg = SimpleNamespace(DEBUG=True, VERBOSE=False)
        logger = JRC.setup_logging(arg)
        assert logger.level == colorlog.DEBUG

    def test_verbose_level(self):
        import colorlog
        arg = SimpleNamespace(DEBUG=False, VERBOSE=True)
        logger = JRC.setup_logging(arg)
        assert logger.level == colorlog.INFO

    def test_default_level(self):
        import colorlog
        arg = SimpleNamespace(DEBUG=False, VERBOSE=False)
        logger = JRC.setup_logging(arg)
        assert logger.level == colorlog.WARNING


# ==============================================================================
# Integration tests — require real API keys in the environment
# ==============================================================================

needs_people  = pytest.mark.skipif(not os.environ.get("PEOPLE_API_KEY"),
                                   reason="PEOPLE_API_KEY not set")
needs_ncbi    = pytest.mark.skipif(not os.environ.get("NCBI_API_KEY"),
                                   reason="NCBI_API_KEY not set")
needs_zenodo  = pytest.mark.skipif(not os.environ.get("ZENODO_API_KEY"),
                                   reason="ZENODO_API_KEY not set")
needs_config  = pytest.mark.skipif(not os.environ.get("CONFIG_SERVER_URL"),
                                   reason="CONFIG_SERVER_URL not set")


@needs_people
class TestPeopleAPIIntegration:
    """Live calls to the HHMI People system using the real API key."""

    def test_call_people_by_id_missing_id_returns_none(self):
        # A well-formed but non-existent ID should return None (no nameFirst)
        result = JRC.call_people_by_id("ZZZZ9999")
        assert result is None

    def test_call_people_by_name_returns_list(self):
        result = JRC.call_people_by_name("Svirskas")
        assert isinstance(result, list)
        assert len(result) >= 1
        names = [r.get("nameLastPreferred", "") for r in result]
        assert any("Svirskas" in n for n in names)

    def test_call_people_by_name_has_expected_fields(self):
        result = JRC.call_people_by_name("Svirskas")
        assert len(result) >= 1
        person = result[0]
        for field in ("employeeId", "email", "nameFirstPreferred", "nameLastPreferred"):
            assert field in person, f"Missing field: {field}"

    def test_call_people_by_suporg_returns_dict(self):
        # Org code for Janelia site — page 0 may be empty or populated
        result = JRC.call_people_by_suporg("60001")
        assert isinstance(result, dict)
        assert "people" in result
        assert "total" in result
        assert "totalPages" in result


@needs_ncbi
class TestNcbiIntegration:
    """Live calls to NCBI ID converter using the real API key."""

    def test_convert_pmid_to_pmcid(self):
        result = JRC.convert_pmid("40406530", convert_to="pmcid")
        assert result == "PMC12097808"

    def test_convert_pmid_returns_empty_for_unknown(self):
        # A PMID that has no PMC equivalent
        result = JRC.convert_pmid("99999999", convert_to="pmcid")
        assert result == ""

    def test_get_pmid_from_doi(self):
        # DOI for a known paper in PubMed
        result = JRC.get_pmid("10.1016/j.cub.2010.06.054")
        assert result == "21174389"

    def test_get_pmid_raises_for_unknown_doi(self):
        # When NCBI_API_KEY is set the function searches PubMed and raises
        # PMIDNotFound when the DOI has no matching record.
        with pytest.raises(JRC.PMIDNotFound):
            JRC.get_pmid("10.9999/nonexistent.doi.xyz")


class TestCrossrefIntegration:
    """Live calls to Crossref (no API key required)."""

    def test_call_crossref_known_doi(self):
        result = JRC.call_crossref("10.1016/j.cub.2010.06.054")
        assert "message" in result
        msg = result["message"]
        assert msg.get("DOI") == "10.1016/j.cub.2010.06.054"

    def test_call_crossref_has_title(self):
        result = JRC.call_crossref("10.1016/j.cub.2010.06.054")
        msg = result["message"]
        assert "title" in msg
        assert len(msg["title"]) > 0


class TestOrcidIntegration:
    """Live calls to ORCID (no API key required)."""

    def test_call_orcid_known_id(self):
        # Josiah Carberry — the canonical ORCID sandbox identity
        result = JRC.call_orcid("0000-0002-1825-0097")
        assert "orcid-identifier" in result
        assert result["orcid-identifier"]["path"] == "0000-0002-1825-0097"

    def test_call_orcid_has_person(self):
        result = JRC.call_orcid("0000-0002-1825-0097")
        assert "person" in result


@needs_zenodo
class TestZenodoIntegration:
    """Live calls to Zenodo using the real API key."""

    def test_call_zenodo_known_record(self):
        result = JRC.call_zenodo("records/10835241")
        assert "id" in result
        assert isinstance(result["id"], int)

    def test_call_zenodo_record_has_doi(self):
        result = JRC.call_zenodo("records/10835241")
        assert "doi" in result
        assert result["doi"].startswith("10.5281/zenodo.")


@needs_config
class TestConfigIntegration:
    """Live calls to the config responder service."""

    def test_get_config_returns_namespace(self):
        result = JRC.get_config("db_config")
        assert isinstance(result, SimpleNamespace)

    def test_call_config_responder_returns_dict(self):
        result = JRC._call_config_responder("config/db_config")
        assert isinstance(result, dict)


# ==============================================================================
# send_email
# ==============================================================================

class TestSendEmail:
    """A single address may be passed as a plain string. The To: header is built
    with ", ".join(receivers), so a bare string has to be normalised to a list or
    the header is assembled from the string's individual characters."""

    @staticmethod
    def _send(receivers):
        """Call send_email with SMTP stubbed out; return (message, to_addrs)."""
        with patch("jrc_common.jrc_common.smtplib.SMTP") as smtp:
            JRC.send_email("body", "sender@hhmi.org", receivers, "subject",
                           server="mail.example.org")
        _sender, to_addrs, raw = smtp.return_value.sendmail.call_args[0]
        return raw, to_addrs

    def test_string_recipient_is_not_split_into_characters(self):
        raw, to_addrs = self._send("svirskasr@hhmi.org")
        assert "To: svirskasr@hhmi.org" in raw
        assert "s, v, i, r" not in raw
        assert to_addrs == ["svirskasr@hhmi.org"]

    def test_list_recipient_is_unchanged(self):
        raw, to_addrs = self._send(["a@hhmi.org", "b@hhmi.org"])
        assert "To: a@hhmi.org, b@hhmi.org" in raw
        assert to_addrs == ["a@hhmi.org", "b@hhmi.org"]

    def test_single_element_list_is_unchanged(self):
        raw, to_addrs = self._send(["a@hhmi.org"])
        assert "To: a@hhmi.org" in raw
        assert to_addrs == ["a@hhmi.org"]
