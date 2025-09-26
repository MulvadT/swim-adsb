"""
Copyright 2019 EUROCONTROL
==========================================

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
   disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
   disclaimer in the documentation and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products
   derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

==========================================

Editorial note: this license is an instance of the BSD license template as provided by the Open Source Initiative:
http://opensource.org/licenses/BSD-3-Clause

Details on EUROCONTROL: http://www.eurocontrol.int
"""
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Tuple, List, Callable, Dict, Optional, Any

from cachetools import cached, TTLCache
from proton import Message

# Official OpenSky client (provided alongside this module)
from swim_adsb.adsb.opensky_api import OpenSkyApi, StateVector, FlightData

_logger = logging.getLogger(__name__)

AirTrafficDataType = Dict[str, Any]


class AirTraffic:
    def __init__(
        self,
        traffic_time_span_in_days: int,
        # Legacy basic auth (optional)
        username: Optional[str] = None,
        password: Optional[str] = None,
        # OAuth2 client credentials (recommended)
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token_url: Optional[str] = None,
        scope: Optional[str] = None,
        # Allow passing a shared requests.Session (optional)
        session: Optional[Any] = None,
        # If True, read missing credentials from environment (see below)
        use_env_credentials: bool = True,
    ):
        """
        Using the OpenSky Network API it tracks the flights from and to specific airports.

        Auth options (choose one):
          1) OAuth2 Client Credentials (recommended / required for non-legacy accounts):
             - client_id, client_secret (and optionally token_url, scope)
          2) Legacy Basic Auth (for legacy accounts only):
             - username, password
          3) Environment variables (if use_env_credentials=True and parameters are missing):
             - OAuth2:  OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET, OPENSKY_TOKEN_URL (optional), OPENSKY_SCOPE (optional)
             - Basic:   OPENSKY_USERNAME, OPENSKY_PASSWORD

        :param traffic_time_span_in_days: Number of days to look back when querying arrivals/departures.
        :param username: Optional OpenSky username (legacy; not used if OAuth2 creds are provided).
        :param password: Optional OpenSky password.
        :param client_id: OAuth2 Client ID.
        :param client_secret: OAuth2 Client Secret.
        :param token_url: OAuth2 token endpoint (defaults to OpenSky official endpoint if not provided).
        :param scope: Optional OAuth2 scope.
        :param session: Optional requests.Session to reuse connections.
        :param use_env_credentials: If True, pull missing creds from environment variables.
        """
        self.traffic_time_span_in_days = traffic_time_span_in_days

        if use_env_credentials:
            # Only fill from env if not explicitly passed
            client_id = client_id or os.getenv("OPENSKY_CLIENT_ID")
            client_secret = client_secret or os.getenv("OPENSKY_CLIENT_SECRET")
            token_url = token_url or os.getenv("OPENSKY_TOKEN_URL")
            scope = scope or os.getenv("OPENSKY_SCOPE")
            username = username or os.getenv("OPENSKY_USERNAME")
            password = password or os.getenv("OPENSKY_PASSWORD")

        # Prefer OAuth2 if available
        if client_id and client_secret:
            self.client = OpenSkyApi(
                client_id=client_id,
                client_secret=client_secret,
                token_url=token_url,
                scope=scope,
                session=session,
            )
            _logger.info("AirTraffic: using OAuth2 client credentials for OpenSky API.")
        else:
            # Fallback to legacy basic auth (supported for legacy accounts)
            self.client = OpenSkyApi(username=username, password=password, session=session)
            if username and password:
                _logger.info("AirTraffic: using legacy Basic Auth for OpenSky API.")
            else:
                _logger.info("AirTraffic: using anonymous access (rate-limited).")

    @property
    def _days_span_in_timestamps(self) -> Tuple[int, int]:
        """
        Returns the timestamp of the start (00:00:00) of the day `traffic_time_span_in_days`
        before the current one and the timestamp of the end (23:59:59) of the current day.

        Timestamps are in seconds since UNIX epoch.
        """
        last_day = datetime.today()
        first_day = last_day - timedelta(days=self.traffic_time_span_in_days)

        start_of_first_day = first_day.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_last_day = last_day.replace(hour=23, minute=59, second=59, microsecond=999999)

        return int(start_of_first_day.timestamp()), int(end_of_last_day.timestamp())

    def _flight_connections_today(self, icao: str, callback: Callable[[str, int, int], Optional[List[FlightData]]]) -> List[FlightData]:
        """
        Returns the flight connections (arrivals or departures based on the callback) within the current time span.

        :param icao: airport identifier (ICAO code)
        :param callback: function to call (self.client.get_arrivals_by_airport or get_departures_by_airport)
        """
        begin, end = self._days_span_in_timestamps

        try:
            result = callback(icao, begin, end) or []
        except Exception as e:
            _logger.error("OpenSky API error for %s between %s and %s: %s", icao, begin, end, e)
            result = []

        return result

    @cached(cache=TTLCache(maxsize=1024, ttl=600))
    def _arrivals_today_handler(self, icao: str) -> List[FlightData]:
        """
        Returns the flight arrivals of the current day span.

        Result is cached for 10 minutes.
        """
        return self._flight_connections_today(icao, callback=self.client.get_arrivals_by_airport)

    @cached(cache=TTLCache(maxsize=1024, ttl=600))
    def _departures_today_handler(self, icao: str) -> List[FlightData]:
        """
        Returns the flight departures of the current day span.

        Result is cached for 10 minutes.
        """
        return self._flight_connections_today(icao, callback=self.client.get_departures_by_airport)

    def _get_states(self) -> List[StateVector]:
        """
        Returns the current list of flight states.
        """
        try:
            states_obj = self.client.get_states()  # returns OpenSkyStates or None
            if states_obj is None:
                return []
            result = states_obj.states or []
        except Exception as e:
            _logger.error("OpenSky API get_states error: %s", e)
            result = []

        return result

    @cached(cache=TTLCache(maxsize=1024, ttl=30))
    def get_states_dict(self, context: Optional[Any] = None) -> Dict[str, StateVector]:
        """
        Builds a dictionary {icao24: StateVector} for quick lookup.
        """
        states = self._get_states()
        return {state.icao24: state for state in states if getattr(state, "icao24", None)}

    def arrivals_handler(self, airport: str, context: Optional[Any] = None) -> Message:
        """
        Callback for arrival-related topics. Returns a Proton Message containing JSON data.
        """
        states_dict = self.get_states_dict()

        data = self._flight_connection_handler(
            airport,
            states_dict=states_dict,
            get_flight_connections_handler=self._arrivals_today_handler
        )

        return Message(body=json.dumps(data), content_type='application/json')

    def departures_handler(self, airport: str, context: Optional[Any] = None) -> Message:
        """
        Callback for departure-related topics. Returns a Proton Message containing JSON data.
        """
        states_dict = self.get_states_dict()

        data = self._flight_connection_handler(
            airport,
            states_dict=states_dict,
            get_flight_connections_handler=self._departures_today_handler
        )

        return Message(body=json.dumps(data), content_type='application/json')

    def _flight_connection_handler(
            self,
            airport: str,
            states_dict: Dict[str, StateVector],
            get_flight_connections_handler: Callable[[str], List[FlightData]]) -> List[AirTrafficDataType]:
        """
        Matches the flight connections (arrivals or departures for the given airport) with the current states
        and returns a subset of the data of those ongoing flights.
        :param airport: ICAO of the airport
        :param states_dict: mapping of icao24 -> current StateVector
        :param get_flight_connections_handler: function to retrieve connections for the airport
        """
        flight_connections = get_flight_connections_handler(airport) or []

        # Map by icao24 (lowercase expected)
        flight_connections_dict: Dict[str, FlightData] = {
            getattr(fc, "icao24", None): fc for fc in flight_connections if getattr(fc, "icao24", None)
        }

        # Keep only connections with a currently tracked state
        flight_connections_with_state = {
            icao24: fc for icao24, fc in flight_connections_dict.items() if icao24 in states_dict
        }

        data = [self._get_flight_data(states_dict.get(fc_icao24), fc)
                for fc_icao24, fc in flight_connections_with_state.items()]

        return data

    @staticmethod
    def _get_flight_data(state: Optional[StateVector], flight_connection: FlightData) -> AirTrafficDataType:
        """
        Combines data of an ongoing flight (live state) with an arrival or departure record and returns a subset.
        """
        # Fallbacks for unknown airports
        from_airport = getattr(flight_connection, "estDepartureAirport", None) or "Unknown airport"
        to_airport = getattr(flight_connection, "estArrivalAirport", None) or "Unknown airport"

        if state is None:
            # Shouldn't happen with our filtering, but be defensive
            return {
                'icao24': getattr(flight_connection, "icao24", None),
                'lat': None,
                'lng': None,
                'from': from_airport,
                'to': to_airport,
                'last_contact': None
            }

        return {
            'icao24': getattr(state, 'icao24', None),
            'lat': getattr(state, 'latitude', None),
            'lng': getattr(state, 'longitude', None),
            'from': from_airport,
            'to': to_airport,
            'last_contact': getattr(state, 'last_contact', None)
        }