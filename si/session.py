"""
This module implements the Session object
"""
import json
import logging
import random
import threading
import time


def get_default_candidate(candidates):
    return {k: v[0] for k, v in candidates.items()}


class Session:
    """
    The Session object which is used to send events to SI servers
    """
    DECIDE_UPPER_LIMIT = 50
    TOKEN_KEY = "ampToken"
    DECISION_KEY = "decision"
    FALLBACK_KEY = "fallback"
    REASON_KEY = "failureReason"

    def __init__(self, amp, **options):
        """
        This method should never be used directly.
        It should only be used indirectly by calling Amp.session
        """
        self.amp = amp
        self._next_event_index = 1  # Only used in via atomic_get_and_increment_next_index
        self._lock = threading.Lock()

        # set the read only properties
        self._user_id = options.get('user_id', amp.user_id)
        if self._user_id is None:
            self._user_id = random_string()
        if "session_id" not in options and self.amp.use_token:
            self._session_id = random_string()
        else:
            self._session_id = options["session_id"]
        self._timeout = options.get('timeout', None)
        self._logger = options.get('logger', amp.logger)
        if self.amp.use_token:
            self._token = ''
        else:
            self._token = 'CUSTOM'

    def __str__(self):
        return '<Session key:%s session_id:%s user_id:%s>' % (self.amp.key, self.session_id,
                                                              self.user_id)

    # Start of Session properties.
    #   user_id (read only)
    #   session_id (read only)
    #   timeout
    #   logger

    @property
    def user_id(self):
        """
        Get read only property user_id
        """
        return self._user_id

    @property
    def session_id(self):
        """
        Get read only property session_id
        """
        return self._session_id

    @property
    def timeout(self):
        if self._timeout is not None:
            return self._timeout
        return self.amp.timeout

    @timeout.setter
    def timeout(self, val):
        """
        Set timeout. Should be in seconds in floating point representation, or None.
        Default value is None, which means use whatever is used by the amp object that created the
        session.
        """
        self._timeout = val

    @property
    def logger(self):
        return self._logger

    @logger.setter
    def logger(self, val):
        """
        Set logger. Should be logging.logger. can't be None
        """
        self._logger = val

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, val):
        """
        Set token. Should be a string.
        Default value is the empty string if self.amp.use_token is True, o.w. 'CUSTOM'
        """
        self._token = val

    # End of Session properties.

    def observe(self, name, properties, **options):
        """
        used to send an observe event to SI servers
        this method does not throw an exception
        """
        result = dict()
        conn = self.get_connection()
        logger: logging._loggerClass = options.get('logger', self.logger)
        try:
            event = self._send_observe_event(conn, name, properties, **options)
            if self.amp.use_token:
                key = Session.TOKEN_KEY
                if key in event:
                    if event[key] != "":
                        result[key] = event[key]
                        self._token = event[key]
            key = Session.REASON_KEY
            if key in event:
                if event[key] != "":
                    result[key] = "amp-agent responded %s" % event[key]
            return result
        except Exception as ex:
            logger.exception('EXCEPTION on observe')
            result[Session.REASON_KEY] = 'client failed because of %s' % ex
            return result

    def decide(self, decision_name, candidates, **options):
        """
        used to send a decide event to SI servers, and to make a decision
        this method does not throw an exception
        """
        return self.decide_internal(decision_name=decision_name, context_name="",
                                    candidates=candidates, with_context=False, properties=None,
                                    options=options)

    def decide_with_context(self, context_name, candidates, decision_name, properties, **options):
        """
        used to send a decide with context request to SI servers, and to make a decision
        this method does not throw an exception
        """
        return self.decide_internal(decision_name=decision_name, context_name=context_name,
                                    candidates=candidates, with_context=True, properties=properties,
                                    options=options)

    def multi_decide_with_context(self, context_name: str, properties, decisions, **options):
        logger: logging._loggerClass = options.get('logger', self.logger)
        reply = {"ampToken": '', "decisions": []}
        for decision_request in decisions:
            candidates = decision_request["candidates"]
            reply["decisions"].append({"decision": get_default_candidate(candidates),
                                       "fallback": True,
                                       "failureReason": "Another decision request failed"})
        if not context_name or not self.user_id or not self.session_id:
            reply['error'] = 'missing context_name, userId or sessionId'
            logger.error(reply['error'])
            return reply
        request = {
            "name": context_name,
            "properties": properties,
            "decisionNames": [],
            "decisions": [],
            "userId": self.user_id,
            "sessionId": self.session_id,
            "index": self.atomic_next_index(),
            "ts": int((time.time() + 0.5) * 1000),
        }
        if self.token != '':
            request["ampToken"] = self.token
        for i, decision_request in enumerate(decisions):
            candidates = decision_request["candidates"]
            candidate_count = 1
            for value in candidates.values():
                candidate_count *= len(value)
            if candidate_count >= Session.DECIDE_UPPER_LIMIT:
                logger.error('Too many candidates: %s is above %s'
                             % (candidate_count, Session.DECIDE_UPPER_LIMIT))
                err = "error': 'using default decision because there are too many candidates %s"
                reply["decisions"][i][Session.REASON_KEY] = err % candidate_count
                return reply
            request["decisionNames"].append(decision_request["name"])
            if isinstance(candidates, (dict,)):
                candidates = [candidates]
            request["decisions"].append({"candidates": candidates, "limit": 1})
        data = json.dumps(request)
        headers = {'Content-type': 'application/json'}
        url = '%s/%s/multiDecideWithContext' % (self.amp.api_path, self.amp.key)
        try:
            net_response = self.get_connection().request('POST', url, data, headers)
        except Exception as ex:
            logger.exception('EXCEPTION on decide')
            reply["error"] = 'using default decision because of error %s' % ex
            return reply
        response = json.loads(net_response)
        if "intent" in response:
            reply["intent"] = response["intent"]
        if self.amp.use_token and response["ampToken"] != '':
            reply["ampToken"] = response["ampToken"]
            self._token = response["ampToken"]
        for i, decision in enumerate(response["decisions"]):
            if "decision" in decision and decision["decision"] != '':
                decision["decision"] = json.loads(decision["decision"])
                reply["decisions"][i] = decision
            else:  # No decision in response is an error. collect all possible info
                reply["decisions"][i]["failureReason"] = \
                    "using default decision because no decision returned from amp-agent"
                reply["decisions"][i]["fallback"] = True
        return reply

    def decide_internal(self, context_name, decision_name, candidates, with_context, properties,
                        options):
        """
        This handles both decide and decide_with_context calls.
        The details are almost the same for each of them.
        """
        result = {Session.DECISION_KEY: get_default_candidate(candidates),
                  Session.FALLBACK_KEY: True}
        conn = self.get_connection()
        logger = options.get('logger', self.logger)  # type: logging._loggerClass
        candidate_count = 1
        for value in candidates.values():
            candidate_count *= len(value)
        if candidate_count >= Session.DECIDE_UPPER_LIMIT:
            logger.error('Too many candidates: %s is above %s' % (candidate_count,
                                                                  Session.DECIDE_UPPER_LIMIT))
            err = "error': 'using default decision because there are too many candidates %s"
            result[Session.REASON_KEY] = err % candidate_count
            return result
        try:
            event = self._send_decide_event(conn, context_name=context_name,
                                            decision_name=decision_name, candidates=candidates,
                                            with_context=with_context, properties=properties,
                                            **options)
            if Session.DECISION_KEY in event:
                result[Session.DECISION_KEY] = json.loads(event[Session.DECISION_KEY])
                key = Session.FALLBACK_KEY
                if key in event:
                    result[key] = event[key]
                key = Session.REASON_KEY
                if key in event:
                    if event[key] != "":
                        result[key] = event[key]
            else:  # No decision in response is an error. collect all possible info
                result[Session.REASON_KEY] = \
                    "using default decision because no decision returned from amp-agent"
                if Session.REASON_KEY in event:
                    result[Session.REASON_KEY] = \
                        result[Session.REASON_KEY] + " because of %s" % event[Session.REASON_KEY]
            if self.amp.use_token:
                key = Session.TOKEN_KEY
                if key in event:
                    if event[key] != "":
                        result[key] = event[key]
                        self._token = event[key]
            if "intent" in event:
                result["intent"] = event["intent"]
            return result
        except Exception as ex:
            logger.exception('EXCEPTION on decide')
            result[Session.REASON_KEY] = 'using default decision because of error %s' % ex
            return result

    def _send_observe_event(self, conn, name, properties, **options):
        """
        used to send an observe event to SI servers
        """
        logger: logging._loggerClass = options.get('logger', self.logger)
        event = Event.create_observe_event(self, name, properties)
        if not event[Event.NAME] or not event[Event.USER_ID] or not event[Event.SESSION_ID] \
                or event[Event.INDEX] <= 0:
            logger.error('missing fields in event %s' % event)
        data = json.dumps(event)
        headers = {'Content-type': 'application/json'}
        url = "%s/%s/observeV2" % (self.amp.api_path, self.amp.key)
        response = conn.request("POST", url, data, headers)
        return json.loads(response)

    def _send_decide_event(self, conn, context_name, decision_name, candidates, with_context,
                           properties, **options):
        """
        used to send a decide event, or a decide with context request, and to make a decision
        """
        logger: logging._loggerClass = options.get('logger', self.logger)
        event = Event.create_decide_event(self, context_name=context_name,
                                          decision_name=decision_name, candidates=candidates)
        if with_context:
            if properties:
                event[Event.PROPERTIES] = properties
            else:
                event[Event.PROPERTIES] = []
        if not event[Event.NAME] or not event[Event.USER_ID] or not event[Event.SESSION_ID] \
                or event[Event.INDEX] <= 0:
            logger.warning('missing fields in event %s' % event)
        data = json.dumps(event)
        headers = {'Content-type': 'application/json'}
        if with_context:
            url = '%s/%s/decideWithContextV2' % (self.amp.api_path, self.amp.key)
        else:
            url = '%s/%s/decideV2' % (self.amp.api_path, self.amp.key)
        response = conn.request('POST', url, data, headers)
        return json.loads(response)

    def atomic_next_index(self):
        """
        Get and increment the next_event_index in a thread safe way
        """
        with self._lock:
            value = self._next_event_index
            self._next_event_index += 1
        return value

    def get_connection(self):
        # use self.user_id to get a number in  [0, len(self.amp.conns)) and then use it as an index
        index = abs(hash(self.user_id)) % len(self.amp.conns)
        conn = self.amp.conns[index]
        return conn


class Event:
    """
    A convenience class for use by the Session class. It represents the events making up a session.
    """
    NAME = 'name'
    DECISION_NAME = 'decisionName'
    PROPERTIES = 'properties'
    DECISION = 'decision'
    INDEX = 'index'
    CLIENT = 'client'
    USER_ID = 'userId'
    SESSION_ID = 'sessionId'
    POLICY_ID = 'policyId'
    CANDIDATES = 'candidates'
    LIMIT = 'limit'
    TS = 'ts'
    AMP_TOKEN = 'ampToken'

    @staticmethod
    def create_observe_event(session, name, properties):
        """
        returns an observe dictionary which can be sent in json form as a REST request
        """
        event = dict()
        event[Event.NAME] = name
        event[Event.PROPERTIES] = properties
        event[Event.USER_ID] = session.user_id
        event[Event.SESSION_ID] = session.session_id
        event[Event.INDEX] = session.atomic_next_index()
        event[Event.CLIENT] = {'python-library': '1.0.0'}
        event[Event.TS] = int((time.time() + 0.5) * 1000)
        if session.token != '':
            event[Event.AMP_TOKEN] = session.token
        return event

    @staticmethod
    def create_decide_event(session, context_name, decision_name, candidates):
        """
        returns a decide dictionary which can be sent in json form as a REST request
        """
        event = dict()
        event[Event.NAME] = context_name
        event[Event.DECISION_NAME] = decision_name
        event[Event.DECISION] = Event.create_decision_dict(candidates)
        event[Event.USER_ID] = session.user_id
        event[Event.SESSION_ID] = session.session_id
        event[Event.INDEX] = session.atomic_next_index()
        event[Event.CLIENT] = {'python-library': '1.0.0'}
        event[Event.TS] = int((time.time() + 0.5) * 1000)
        if session.token != '':
            event[Event.AMP_TOKEN] = session.token
        return event

    @staticmethod
    def create_decision_dict(candidates):
        """
        helper method for create_decide_event
        """
        decide = dict()
        if isinstance(candidates, (dict,)):
            decide[Event.CANDIDATES] = [candidates]  # cross product case
        else:
            decide[Event.CANDIDATES] = candidates
        decide[Event.LIMIT] = 1
        return decide


DEFAULT_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"


def random_string(length=16, charset=DEFAULT_CHARSET):
    """
    Returns a random string of the given length using characters from the given charset.
    """
    return ''.join(random.choice(charset) for _ in range(length))
