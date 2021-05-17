import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout, SSLError
import json
import uuid
import sys
import traceback
import time
import logging
import random

import logging
log = logging.getLogger()
log.addHandler(logging.NullHandler())
log.setLevel(logging.DEBUG)


_DEFAULT_CREATE_TIMEOUT = 30 * 60
_DEFAULT_DELETE_TIMEOUT = 30 * 60
_DEFAULT_UPDATE_TIMEOUT = 30 * 60

class RemoteError(IOError):
    retry_modes = frozenset(['TERMINAL', 'RETRIABLE', 'RETRIABLE_FOREVER'])

    def __init__(self, code, msg, retry_mode='RETRIABLE'):
        super(RemoteError, self).__init__(code, msg)
        if not retry_mode in RemoteError.retry_modes:
            raise ValueError("Invalid retry mode: %s" % retry_mode)
        self.retry_mode = retry_mode


def _extract_http_error(resp):
    if resp.status_code == 503:
        retry_mode = 'RETRIABLE_FOREVER'
    elif resp.status_code < 500 and resp.status_code not in (404, 408):
        retry_mode = 'TERMINAL'
    else:
        retry_mode = 'RETRIABLE'

    return RemoteError(resp.status_code, "HTTP Error %s : %s" % (resp.status_code, resp.text), retry_mode)


def exponential_backoff(max_tries, max_sleep=20):
    """
    Returns a series of floating point numbers between 0 and min(max_sleep, 2^i-1) for i in 0 to max_tries
    """
    return [random.random() * min(max_sleep, (2 ** i - 1)) for i in range(0, max_tries)]


def extend_backoff(durations, max_sleep=20):
    """
    Adds another exponential delay time to a list of delay times
    """
    durations.append(random.random() * min(max_sleep, (2 ** len(durations) - 1)))


def retry_on_failure(max_tries=5, http_error_extractor=_extract_http_error):
    def _decorate(f):
        def _retry(*args, **kwargs):
            durations = exponential_backoff(max_tries)
            for i in durations:
                if i > 0:
                    log.debug("Sleeping for %f seconds before retrying", i)
                    time.sleep(i)

                try:
                    return f(*args, **kwargs)
                except SSLError as e:
                    log.exception("SSLError")
                    raise RemoteError(None, str(e), retry_mode='TERMINAL')
                except ConnectionError as e:
                    log.exception("ConnectionError")
                    last_error = RemoteError(None, str(e))
                except HTTPError as e:
                    last_error = http_error_extractor(e.response)
                    if last_error.retry_mode == 'TERMINAL':
                        raise last_error
                    elif last_error.retry_mode == 'RETRIABLE_FOREVER':
                        extend_backoff(durations)

                    log.exception(last_error.strerror)
                except Timeout as e:
                    log.exception("Timeout")
                    last_error = RemoteError(None, str(e))
            else:
                raise last_error
        return _retry
    return _decorate


class CustomResource(object):
    def __init__(self, event):
        self._event = event
        self._logicalresourceid = event.get("LogicalResourceId")
        self._physicalresourceid = event.get("PhysicalResourceId")
        self._requestid = event.get("RequestId")
        self._resourceproperties = event.get("ResourceProperties")
        self._resourcetype = event.get("ResourceType")
        self._responseurl = event.get("ResponseURL")
        self._requesttype = event.get("RequestType")
        self._servicetoken = event.get("ServiceToken")
        self._stackid = event.get("StackId")
        self._region = self._get_region()
        self.result_text = None
        self.result_attributes = None

        # Set timeout for actions
        self._create_timeout = _DEFAULT_CREATE_TIMEOUT
        self._delete_timeout = _DEFAULT_DELETE_TIMEOUT
        self._update_timeout = _DEFAULT_UPDATE_TIMEOUT

    @property
    def logicalresourceid(self):
        return self._logicalresourceid

    @property
    def physicalresourceid(self):
        return self._physicalresourceid

    @property
    def requestid(self):
        return self._requestid

    @property
    def resourceproperties(self):
        return self._resourceproperties

    @property
    def resourcetype(self):
        return self._resourcetype

    @property
    def responseurl(self):
        return self._responseurl

    @property
    def requesttype(self):
        return self._requesttype

    @property
    def servicetoken(self):
        return self._servicetoken

    @property
    def stackid(self):
        return self._stackid

    def create(self):
        return {}

    def delete(self):
        return {}

    def update(self):
        return {}

    def _get_region(self):
        if 'Region' in self._resourceproperties:
            return self._resourceproperties['Region']
        else:
            return self._stackid.split(':')[3]

    def determine_event_timeout(self):
        if self.requesttype == "Create":
            timeout = self._create_timeout
        elif self.requesttype == "Delete":
            timeout = self._delete_timeout
        else:
            timeout = self._update_timeout

        return timeout

    def process_event(self):
        if self.requesttype == "Create":
            command = self.create
        elif self.requesttype == "Delete":
            command = self.delete
        else:
            command = self.update

        try:
            self.result_text = command()
            success = True
            if isinstance(self.result_text, dict):
                try:
                    self.result_attributes = { "Data" : self.result_text }
                    log.info("Command %s-%s succeeded", self.logicalresourceid, self.requesttype)
                    log.debug("Command %s output: %s", self.logicalresourceid, self.result_text)
                except:
                    log.error("Command %s-%s returned invalid data: %s", self.logicalresourceid,
                              self.requesttype, self.result_text)
                    success = False
                    self.result_attributes = {}
            else:
                raise ValueError("Results must be a JSON object")
        except:
            e = sys.exc_info()
            log.error("Command %s-%s failed", self.logicalresourceid, self.requesttype)
            log.debug("Command %s output: %s", self.logicalresourceid, e[0])
            log.debug("Command %s traceback: %s", self.logicalresourceid, traceback.print_tb(e[2]))
            success = False

        self.send_result(success, self.result_attributes)

    def send_result(self, success, attributes):
        attributes = attributes if attributes else {}
        source_attributes = {
            "Status": "SUCCESS" if success else "FAILED",
            "StackId": self.stackid,
            "RequestId": self.requestid,
            "LogicalResourceId": self.logicalresourceid
        }

        source_attributes['PhysicalResourceId'] = self.physicalresourceid
        if not source_attributes['PhysicalResourceId']:
            source_attributes['PhysicalResourceId'] = str(uuid.uuid4())

        if not success:
            source_attributes["Reason"] = "Unknown Failure"

        source_attributes.update(attributes)
        log.debug("Sending result: %s", source_attributes)
        self._put_response(source_attributes)

    @retry_on_failure(max_tries=10)
    def __send(self, data):
        requests.put(self.responseurl,
                     data=json.dumps(data),
                     headers={"Content-Type": ""},
                     verify=True).raise_for_status()

    def _put_response(self, data):
        try:
            self.__send(data)
            log.info("CloudFormation successfully sent response %s", data["Status"])
        except IOError as e:
            log.exception("Failed sending CloudFormation response")

    def __repr__(self):
        return str(self._event)
