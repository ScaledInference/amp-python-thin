"""
Top level module used to create an amp object, which represents a project.
"""

import logging


from . import smart_conn
from . import session


class AmpError(Exception):
    """ The error that the library throws. """


class Amp:
    """
    The Amp class, which is used to create an object representing a project.
    """

    def __init__(self, key, amp_agents, **options):
        if not key:
            raise AmpError("'key' can't not be empty")
        if not amp_agents:
            raise AmpError("'amp_agents' can't be empty")

        # Set all the properties to initial values.
        self._project_key = key
        self._amp_agents = amp_agents
        self._api_path = "/api/core/v2"
        self._user_id = options.get("user_id", None)
        self._timeout = options.get("timeout", 10.0)  # 10 second timeout
        self._reconnect_timeout = options.get("reconnect_timeout", 10.0)  # 10 second timeout
        logging.basicConfig()
        self._logger = options.get("logger", logging.getLogger('amp'))
        self._use_token = options.get("use_token", True)
        self._session_lifetime = options.get("session_lifetime", 1800)
        self.conns = []

        for amp_agent in self._amp_agents:
            https, host, port = Amp.parse_agent(amp_agent)
            conn = smart_conn.SmartConn(self._logger, https, host, port, self._timeout,
                                        self._reconnect_timeout)
            url = '/test/update_from_spa/' + self._project_key + \
                  "?session_life_time=%s" % self._session_lifetime
            response = conn.request('GET', url)
            if response != 'Key is known':
                raise AmpError('got response text %s. Needs to be "Key is known"' % response)
            self.conns.append(conn)

    @staticmethod
    def parse_agent(agent_str):
        """ Parse the Amp agent string. """
        arr = agent_str.split('://')
        if len(arr) != 2:
            raise AmpError('bad amp agent %s' % agent_str)
        protocol = arr[0].lower()
        if protocol not in ["http", "https"]:
            raise AmpError("method in %s must be 'http' or 'https'" % agent_str)
        https = protocol == 'https'
        last_component = arr[-1]
        host_info = last_component.rsplit(':', 1)
        if len(host_info) == 1:
            host, port = host_info[0], 8100
        else:
            host, port = tuple(host_info)
        return https, host, int(port)

    def __str__(self):
        return "<Amp project_key:%s>" % self._project_key

    # Start of Amp properties.
    #   api_path
    #   key
    #   domain
    #   user_id
    #   timeout
    #   logger

    @property
    def api_path(self):
        """ Get api_path. """
        return self._api_path

    @api_path.setter
    def api_path(self, val):
        """ Set api_path. Should be a string like "/api/core/v1" """
        self._api_path = val

    @property
    def key(self):
        """ Get key. """
        return self._project_key

    @key.setter
    def key(self, val):
        """ Set key, i.e. the project key. """
        self._project_key = val

    @property
    def amp_agents(self):
        """ Get amp_agents. """
        return self._amp_agents

    @amp_agents.setter
    def amp_agents(self, val):
        """ Set amp_agents, i.e. location of servers e.g ["server-0:8100","server-1:8100"] """
        self._amp_agents = val

    @property
    def user_id(self):
        """ Get user_id. """
        return self._user_id

    @user_id.setter
    def user_id(self, val):
        """ Set user_id. """
        self._user_id = val

    @property
    def timeout(self):
        """ Get timeout """
        return self._timeout

    @timeout.setter
    def timeout(self, val):
        """ Set timeout. Should be in seconds in floating point representation. """
        self._timeout = val

    @property
    def logger(self):
        """ Get logger. """
        return self._logger

    @logger.setter
    def logger(self, val):
        """
        Set logger. Should be logging.logger. Default value is None.
        """
        self._logger = val

    @property
    def use_token(self):
        """ Get token """
        return self._use_token

    @use_token.setter
    def use_token(self, val):
        """ Set use_token. Should be true (use amp tokens) or false (aka "custom"). """
        self._use_token = val

    # End of Amp properties.

    def session(self, **options):
        """ Create a session object for the project represented by the amp object """
        return session.Session(amp=self, **options)
