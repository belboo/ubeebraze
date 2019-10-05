# -*- coding: utf-8 -*-
"""Braze API Client

This is a simple `Braze API`_ client for updating user data and sending track events for CRM.
Idea is to use it as a sink for identify and track calls without managing the API calls directly.
The definition of the API allows for one call to contain max 75 user objects and 75 track events.
This is taken into account and events and users are accumulated in the queue and flushed as necessary.
Todo: add TTL for flushing

Example:
    Todo: Add invocation example

Contents:
    Braze Config:
        Holds basic configuration for the client
        Todo: add support for environment vars and read from file

    User Class:
        Class abstracting a user object in Braze. Holds arbitrary traits.

    Event Class:
        Class abstracting an event. Can hold arbitrary properties dictionary.

    BrazeClient:
        Braze client to abstract the API interaction.

Todo:
    * Not forget documentation
    * Replace the stupid pause property with proper rate-limiter (probably unnecessary)
    * Replace the local file log with a logging logger
    * Add schema dictionary
    * Add Pandas DF ingestion functions

.. _Braze API:
   https://www.braze.com/docs/api/basics/
"""

import sys
import pandas as pd
import time
import requests
import json
from typing import List
from datetime import datetime


BRAZE_DEFAULT_ENDPOINT = 'https://rest.fra-01.braze.eu'
BRAZE_DEFAULT_API_KEY = 'APIKEY'
BRAZE_DEFAULT_BATCH_SIZE = 75
BRAZE_DEFAULT_AUTOFLUSH = True
BRAZE_DEFAULT_SEND = False
BRAZE_DEFAULT_PAUSE = 0
BRAZE_DEFAULT_LOG = True
BRAZE_DEFAULT_LOG_FILE = None


class BrazeConfig:
    """Braze client configuration
    """

    def __init__(self,
                 endpoint: str,
                 api_key: str,
                 batch_size: int = BRAZE_DEFAULT_BATCH_SIZE,
                 auto_flush: bool = BRAZE_DEFAULT_AUTOFLUSH,
                 send: bool = BRAZE_DEFAULT_SEND,
                 pause: float = BRAZE_DEFAULT_PAUSE,
                 log: bool = BRAZE_DEFAULT_LOG):

        """Initialize Braze configuration

        Args:
            endpoint (str): Braze endpoint URL.
            api_key (str): API key for the app group.
            batch_size (int): Number of users and events to batch in one API call.
            auto_flush (bool): Automatically flush the queue when it exceeds the batch length.
            send (bool): Must be True in order for the API calls to be actually made. Use False for debugging.
            pause (float): Time in second to sleep after.
            log (str): File to write API call logs to (silent if None).
        """

        self.endpoint = endpoint
        self.api_key = api_key
        self.batch_size = batch_size
        self.auto_flush = auto_flush
        self.send = send
        self.pause = pause
        self.log = log

    def __str__(self):
        s = 'BrazeConfig\n'
        s += ' endpoint: {}\n'.format(self.endpoint)
        s += ' api_key: {}\n'.format(self.api_key)
        s += ' batch_size: {}\n'.format(self.batch_size)
        s += ' auto_flush: {}\n'.format(self.auto_flush)
        s += ' send: {}\n'.format(self.send)
        s += ' pause: {}\n'.format(self.pause)
        s += ' log: {}\n'.format(self.log)

        return s

    def __repr__(self):
        s = '<BrazeConfig[{},{},batch={}, autoflush={}, send={}, pause={}{}, log={}]>\n' \
            .format(self.endpoint, self.api_key,
                    self.batch_size, self.auto_flush,
                    self.send, self.pause, 's' if self.pause else '',
                    self.log)

        return s


class BrazeUser:
    """ Class abstracting a user entity.

    Can keep track of which client it belongs to and enqueue itself on demand.
    """

    def __init__(self,
                 external_id: str = None,
                 update_existing_only: bool = False,
                 braze: object = None,
                 **kwargs: dict):

        """Initialise a BrazeUser

        Args:
            external_id (str): External ID of the user.
            update_existing_only (bool): Whether the client should refraing from creating this user in Braze if it does not exist.
            braze (BrazeClient): BrazeClient to use for enqueueing the BrazeUser
            **kwargs (dict): Traits to be assigned to the BrazeUser
        """

        self.external_id = external_id
        self.update_existing_only = update_existing_only

        self.traits = dict()
        self.traits.update(kwargs)

        self.braze = braze

    def __str__(self) -> str:

        s = 'BrazeUser {}'.format(self.external_id)
        s += ''.join(['\n {}: {}'.format(k, v) for k, v in self.traits.items()])
        return s

    def __repr__(self) -> str:

        return '<BrazeUser[{}]>'.format(self.external_id)

    def set(self, **kwargs) -> object:
        """Generic user trait setter. Simply updates the BrazeUser trait dictionary.

        Args:
            **kwargs (dict): Any number of K-V arguments.

        Returns:
            Returns self for chaining operations.
        """

        self.traits.update(kwargs)
        return self

    def as_dict(self) -> dict:
        """Generate a dictionary of BrazeUser properties to be passed to the API request invocation."""

        user_dict = dict(
            external_campaign_id=self.external_id,
            update_existing_only=self.update_existing_only)

        try:
            for k, v in self.traits.items():
                if v is None:
                    user_dict[k] = None
                elif isinstance(v, datetime):
                    user_dict[k] = v.isoformat()
                else:
                    user_dict[k] = v
        except (ValueError, IndexError):
            sys.stderr.write('Failed to dictionarise {}:\n{}'.format(self.external_id, str(self)))

        return user_dict

    def enqueue(self, braze: object = None):
        """Enqueue the BrazeUser with the Braze client to be sent to Braze.

        Requires BrazeUser to know its parent BrazeClient handler...

        Args:
            braze (BrazeClient): BrazeClient to enqueue the BrazeUser with
                (overrides the one set in the BrazeUser object if any)
        """

        if braze is None and self.braze is None:
            return ValueError('Braze API handler neither set nor passed as parameter')

        if braze is not None:
            braze.enqueue(self)
        else:
            self.braze.enqueue(self)


class BrazeEvent:
    """ Class abstracting an event entity.

        Can keep track of which client it belongs to and enqueue itself on demand.
    """

    def __init__(self,
                 external_id: str,
                 name: str,
                 timestamp: datetime = None,
                 braze: object = None,
                 **kwargs: dict):

        """Initialise a Braze Event

        Args:
            external_id (str): External ID of the user the event relates to.
            name (str): Name of the event.
            timestamp (datetime): Timestamp of the event.
            braze (BrazeClient): Braze client that the event should be enqueued by.
            **kwargs (dict): Set of properties to be assigned to the event.
        """
        self.external_id = external_id
        self.name = name

        self.timestamp = datetime.utcnow() if timestamp is None else timestamp

        self.properties = dict()
        self.properties.update(kwargs)

        self.braze = braze

    def __str__(self):

        s = 'Braze event {} for {}'.format(self.name, self.external_id)
        if self.traits:
            s += ''.join(['\n {}: {}'.format(k, v) for k, v in self.traits.items()])
        return s

    def __repr__(self):

        return '<BrazeEvent[{}]>'.format(self.external_id)

    def set(self, **kwargs):
        """Generic event property setter. Simply updates the BrazeEvent property dictionary.

        Args:
            **kwargs (dict): Any number of K-V arguments to set as properties of the event.

        Returns:
            Returns self for chaining operations.
        """

        self.properties.update(kwargs)
        return self

    def as_dict(self):
        """Generate a dictionary of BrazeEvent properties to be passed to the API request invocation."""

        event_dict = dict(
            external_campaign_id=self.external_id,
            name=self.name,
            time=self.timestamp.isoformat() if isinstance(self.timestamp, datetime) else self.timestamp)

        if self.properties:
            event_dict['properties'] = {}
            try:
                for k, v in self.properties.items():
                    if v is None:
                        event_dict['properties'][k] = None
                    elif isinstance(v, datetime):
                        event_dict['properties'][k] = v.isoformat()
                    else:
                        event_dict['properties'][k] = v
            except (ValueError, IndexError):
                sys.stderr.write('Failed to dictionarise {}:\n{}'.format(self.external_id, str(self)))

        return event_dict

    def enqueue(self, braze=None):
        """Enqueue the BrazeUser with the Braze client to be sent to Braze.

        Requires BrazeUser to know its parent BrazeClient handler...

        Args:
            braze (BrazeClient): BrazeClient to enqueue the BrazeUser with
                (overrides the one set in the BrazeUser object if any)
        """

        if braze is None and self.braze is None:
            return ValueError('Braze handler neither set nor passed as parameter')

        if braze is not None:
            braze.enqueue(self)
        else:
            self.braze.enqueue(self)


class BrazeCampaign:
    """ Class abstracting Braze campaign interaction.

        Can store campaign data or trigger the campaign for a set of users.
    """

    def __init__(self,
                 campaign_id: str,
                 name: str,
                 is_api: bool = False,
                 tags: list = None,
                 braze: object = None):

        """Initialise a Braze Campaign

        Args:
            campaign_id (str): Campaign ID / APIKEY.
            name (str): Name of the campaign.
            is_api (bool): Campaign is api-triggered.
            tags (list): List of tags of the campaign.
            braze (BrazeClient): Braze client that the event should be enqueued by.
        """

        self.campaign_id = campaign_id
        self.name = name

        self.is_api = is_api
        self.tags = tags

        self.braze = braze

        self.data_series = None
        self.details = None

    def __str__(self):

        s = 'Braze campaign {} with ID {}'.format(self.name, self.campaign_id)
        s += '\n - api-triggered: {}'.format('yes' if self.is_api else 'no')
        s += '\n - tags: {}'.format(', '.join(self.tags)) if self.tags is not None else ''
        return s

    def __repr__(self):

        return '<BrazeCampaign[{},{}]>'.format(
            self.campaign_id,
            self.name if len(self.name) < 50 else (self.name[:47] + '...'))

    def as_dict(self):
        """Generate a dictionary of BrazeCampaign data."""

        return {}

    def get_data_series(self, n: int = 7, end_time: datetime = None) -> list:
        """Fetch campaign data series.

        Args:
            n (int): Length of the data series to fetch.
            end_time (datetime): Ending time of the data series.

        Returns:
            Series data as list or dictionaries.
        """

        if self.braze is None:
            return None

        url = self.braze.config.endpoint + '/campaigns/data_series'
        params = {
            'api_key': self.braze.config.api_key,
            'campaign_id': self.campaign_id,
            'length': n
        }

        if end_time is not None:
            params['ending_at'] = end_time.isoformat()

        s = requests.Session()
        r = s.get(url=url, params=params)

        if r.status_code == 200:
            self.data_series = json.loads(r.content).get('data')
            return self.data_series

        return None

    def get_details(self) -> dict:
        """Fetch campaign details.

        Returns:
            Dictionary of campaign properties.
        """

        if self.braze is None:
            return None

        url = self.braze.config.endpoint + '/campaigns/details'
        params = {
            'api_key': self.braze.config.api_key,
            'campaign_id': self.campaign_id
        }

        s = requests.Session()
        r = s.get(url=url, params=params)

        if r.status_code == 200:
            self.details = json.loads(r.content)
            return self.details

        return None


class BrazeClient:
    """Braze API Helper Client Class

    Takes care of enqueueing the

    """

    def __init__(self,
                 endpoint: str = BRAZE_DEFAULT_ENDPOINT,
                 api_key: str = BRAZE_DEFAULT_API_KEY,
                 batch_size: int = BRAZE_DEFAULT_BATCH_SIZE,
                 auto_flush: bool = BRAZE_DEFAULT_AUTOFLUSH,
                 send: bool = BRAZE_DEFAULT_SEND,
                 pause: float = BRAZE_DEFAULT_PAUSE,
                 log: bool = BRAZE_DEFAULT_LOG,
                 log_file: str = BRAZE_DEFAULT_LOG_FILE):

        """Initialize BrazeClient with initial BrazeConfig values.

        Args:
            endpoint (str): Braze endpoint URL.
            api_key (str): API key for the app group.
            batch_size (int): Number of users and events to batch in one API call.
            auto_flush (bool): Automatically flush the queue when it exceeds the batch length.
            send (bool): Must be True in order for the API calls to be actually made. Use False for debugging.
            pause (float): Time in second to sleep after.
            log_file (str): File to write API call logs to.
            log (bool): Write flush logs.
        """

        if log is False and log_file is None:
            self.log = None
        else:
            if log_file is not None:
                self.log = log_file
            else:
                self.log = 'braze_{}.log'.format(datetime.utcnow().strftime('%Y-%m-%d_%Hh%Mm%Ss'))

        self.config = BrazeConfig(
            endpoint=endpoint,
            api_key=api_key,
            batch_size=batch_size,
            auto_flush=auto_flush,
            send=send,
            pause=pause,
            log=self.log)

        self.requests = {}

        # Using deque might be elegant, but not that useful...
        # self.event_queue = collections.deque()
        # self.user_queue = collections.deque()

        self.event_queue = list()
        self.user_queue = list()
        self.campaigns = []
        self.canvases = []

    def __str__(self):

        if self.config is None:
            return None

        s = 'Braze API Helper\n endpoint: {}\n api-key: {}' \
            .format(self.config.endpoint, self.config.api_key)

        if self.user_queue:
            s += '\n users enqueued: {}'.format(len(self.user_queue))

        if self.event_queue:
            s += '\n events enqueued: {}'.format(len(self.event_queue))

        return s

    def __repr__(self):

        return ('<Braze[{}, {}, nu={}, ne={}]' \
                .format(self.config.endpoint,
                        self.config.api_key,
                        len(self.user_queue),
                        len(self.event_queue))
                if self.config is not None else None)

    def user(self, external_id: str, *args, **kwargs):
        """Proxy function to add a BrazeUser"""

        return BrazeUser(external_campaign_id=external_id, *args, braze=self, **kwargs)

    def event(self, external_id: str, name: str, timestamp: datetime = None, *args, **kwargs):
        """Proxy function to add a BrazeEvent"""

        return BrazeEvent(external_campaign_id=external_id, name=name, timestamp=timestamp, braze=self, **kwargs)

    def enqueue(self, obj):
        """Add object (user or event) to the client queue."""

        if isinstance(obj, BrazeUser):
            self.user_queue.append(obj)

        elif isinstance(obj, BrazeEvent):
            self.event_queue.append(obj)

        else:
            raise ValueError('Object passed is neither a BrazeUser nor a BrazeEvent')

        if self.config.auto_flush \
                and ((len(self.user_queue) >= self.config.batch_size) \
                     or (len(self.event_queue) >= self.config.batch_size)):
            self.flush()

    def flush(self, flush_users: bool = True, flush_events: bool = True):
        """Flush user and/or event queues.

        Sends all enqueued users and events ti Braze.

        Args:
            flush_users (bool): Flush user queue?
            flush_events (bool): Flush event queue?
        """

        s = requests.Session()

        flush_time = datetime.utcnow()

        while (flush_users and self.user_queue) or (flush_events and self.event_queue):

            request_body = {
                'api_key': self.config.api_key,
            }

            user_batch_size = 0
            event_batch_size = 0

            if flush_users and self.user_queue:
                request_body['attributes'] = list()

                user_batch_size = min(len(self.user_queue), self.config.batch_size)

                for user in self.user_queue[0:user_batch_size]:
                    request_body['attributes'].append(user.as_dict())

            if flush_events and self.event_queue:
                request_body['events'] = list()

                event_batch_size = min(len(self.event_queue), self.config.batch_size)

                for event in self.event_queue[0:event_batch_size]:
                    request_body['events'].append(event.as_dict())

            if self.log is not None:
                log = open(self.log, 'a')
                log.write('"{} Request {}"\n'.format('-' * 2, '-' * 49))
                log.write('"{} {} {}"\n\n'.format('-' * 2, datetime.utcnow().isoformat(), '-' * 30))
                log.write(json.dumps(request_body, indent=4))
                log.write('\n\n')

            self.requests[flush_time] = {
                'request': request_body,
                'url': self.config.endpoint + '/users/track',
                'timestamp': flush_time}

            if self.config.send:
                r = s.post(url=self.config.endpoint + '/users/track', json=request_body)
                resp = json.loads(r.content)

                self.requests[flush_time]['ok'] = r.ok
                self.requests[flush_time]['status'] = r.status_code
                self.requests[flush_time]['response'] = resp

                if self.log is not None:
                    log.write('"{} Response {}"\n\n'.format('-' * 2, '-' * 48))
                    log.write(json.dumps(resp, indent=4))
                    log.write('\n\n')

                if self.config.send and resp.get('error') is not None:
                    raise requests.RequestException(
                        'API call to Braze failed with "{}"'.format(resp.get('error')))

            # If sent discard the sent user and event batches from the queue
            if user_batch_size > 0:
                self.user_queue = self.user_queue[user_batch_size:]

            # If sent discard the sent even batches from the queue
            if event_batch_size > 0:
                self.event_queue = self.event_queue[event_batch_size:]

            if self.log is not None:
                log.close()

            # Wait a bit before continuing. Would be useful between uploading a user and the first campaign.
            if self.config.pause:
                time.sleep(self.config.pause)

        s.close()

    def add_campaign(self, campaign_id: str, name: str, is_api: bool = False, tags: list = None):
        """Initialise a Braze Campaign

        Args:
            id (str): Campaign ID / APIKEY.
            name (str): Name of the campaign.
            is_api (bool): Campaign is api-triggered.
            tags (list): List of tags of the campaign.
        """

        self.campaigns.append(BrazeCampaign(
            campaign_id=campaign_id,
            name=name,
            is_api=is_api,
            tags=tags,
            braze=self
        ))

    def get_campaigns(self, archived: bool = False, desc: bool = False) -> List[BrazeCampaign]:
        """Fetch campaign list from braze.

        Args:
            archived (bool): Also list archived campaigns?
            desc (bool): Sort campaigns in descending order (default: ascending)

        Returns:
            Returns an array of BrazeCampaigns.
        """

        url = self.config.endpoint + '/campaigns/list?api_key={}'.format(self.config.api_key)
        if archived:
            url += '&include_archived=true'
        if desc:
            url += '&sort_order=desc'

        s = requests.Session()
        r = s.get(url=url)

        self.campaigns = []

        if r.status_code == 200:
            resp = json.loads(r.content)
            for campaign in resp.get('campaigns', []):
                self.campaigns.append(BrazeCampaign(
                    campaign_id=campaign.get('id', 'n/a'),
                    name=campaign.get('name', 'n/a'),
                    is_api=campaign.get('is_api_campaign', False),
                    tags=campaign.get('tags'),
                    braze=self
                ))

        return self.campaigns
