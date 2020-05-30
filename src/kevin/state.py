import threading
import requests
import jsonify
import json
import time
import random
import logging
import concurrent.futures

from time import sleep
from enum import Enum, auto, unique

# endpoints
MESSAGE = '/message'
TOPIC = '/topic'
STATUS = '/status'
APPEND = '/append'
ELECTION = '/election'

# header and ip
HEADERS = {'Content-Type': 'application/json'}
LOCALHOST = 'http://127.0.0.1'

# timeouts in ms
ELECTION_LOW = 0.150
ELECTION_HIGH = 0.300
HEARTBEAT = 0.050

random.seed()

# logging.basicConfig(
#         level=logging.DEBUG,
#         format='[%(levelname)s] (%(threadName)-3s) %(message)s'
# )

@unique
class Role(Enum):
    LEADER = auto()
    FOLLOWER = auto()
    CANDIDATE = auto()

class State:
    def __init__(self, ip, port, nodes):
        # info about the node
        self.ip = ip
        self.port = port
        self.nodes = nodes
        self.role = Role.FOLLOWER

        # vote information
        self.election_time = time.time()
        self.majority = ((len(self.nodes) + 1) // 2) + 1
        self.vote_count = 0
        self.election_flag = False

        # Persistent state on all servers. Updated on stable storage before responding to requests
        self.current_term = 0
        self.log = [None]

        # Volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0
        # implement message queue as a dictionary with topic as key and a queue as value - message_queue<Topic, Queue>
        self.message_queue = {}

        # Volatile state on leaders. Reinitialized after election
        self.next_index = None
        self.match_index = 0

        self.init()

    def init(self):
        self.reset_election_timer()
        self.start_election_thread()


    def reset_election_timer(self):
        """Change the election time to some time in the future by a random time between 150-300ms"""
        self.election_time = time.time() + random.uniform(ELECTION_LOW, ELECTION_HIGH)

    
    def count_vote(self):
        # logging.debug(f'{time.time()} One point for Gryffindor')
        self.vote_count += 1
        if self.vote_count >= self.majority:
            self.election_flag = False
            self.role = Role.LEADER
            # logging.debug(f'{time.time()} {self.port} became leader')
            self.start_append()
            

    def start_election(self):
        """Launches a pool of threads to request votes from all other nodes using executor for asynchronous execution."""
        self.current_term += 1
        self.vote_count = 0
        self.count_vote()
        self.init()

        if len(self.nodes) > 0:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
                    future_election = {executor.submit(self.request_vote, int(node), 60): node for node in self.nodes}
                    for future in concurrent.futures.as_completed(future_election):
                        if self.role != Role.CANDIDATE:
                            break
                        else:
                            data = future.result()
                            # data can be a Null result if an exception was thrown (requested node was down)
                            if data:
                                # count vote if still candidate and they casted successful vote
                                if data.json()['vote_granted'] and self.role == Role.CANDIDATE:
                                    self.count_vote()
                                # if vote was not cast, then two possibilities:
                                #   1. Already voted for self
                                #   2. The candidate is not the most up to date - in which case the candidate must update its term and switch to follower
                                elif not data.json()['vote_granted']:
                                    if data.json()['term'] > self.current_term:
                                        self.current_term = data.json()['term']
                                        self.role = Role.FOLLOWER
                                        break


    def start_election_thread(self):
        """ Helper function to launch a thread for continous election launches """
        election_thread = threading.Thread(target=self.election_loop,
                                            name='election_loop_'+str(self.port))
        election_thread.start() 


    def election_loop(self):
        """ Until become leader, continuously check the election timeout to see if election should be launched """
        while self.role != Role.LEADER:
            if self.election_time - time.time() <= 0 and not self.election_flag:
                self.role = Role.CANDIDATE
                # logging.debug(f'{self.port} became candidate')
                self.election_flag = True
                self.start_election()


    def request_vote(self, node, timer):
        data = {'term': self.current_term, 'candidate_id': self.port}
        # try to request a vote. it could fail if the node has died. if so, just return None
        try:
            res = requests.post(f'{LOCALHOST}:{node}{ELECTION}', json=data, headers=HEADERS)
            # logging.debug(f'{time.time()} {res.text}')
            return res
        except requests.exceptions.ConnectionError:
            return None


    def respond_vote(self, term):
        """ Automatically send a no if current term is greater than the term received from candidate """
        if self.current_term <= term:
            self.reset_election_timer()
            self.current_term = term
            return self.current_term, True
        return self.current_term, False


    def respond_append(self, term):
        """ Automatically send no if current term is greater than term from leader. Else, set own term and change to follower """
        # leader could receive append message but if term is higher, then ignore and send back false and own term
        if term < self.current_term:
            self.reset_election_timer()
            return self.current_term, False
        else:
            self.current_term = term
            # if leader and receive an append from leader of higher term, this is erroneous. need to step down to follower and restart election thread.
            if self.role == Role.LEADER:
                self.role = Role.FOLLOWER
                self.election_flag = False
                self.init()
            # if candidate, it means other candidate has been elected and need to become follower
            elif self.role == Role.CANDIDATE:
                self.role = Role.FOLLOWER
                self.election_flag = False
            # if follower, treat as heartbeat message and reset election timer
            else:
                self.reset_election_timer()
            return self.current_term, True

    
    def start_append(self):
        """ Helper function to launch threead to start appending once become leader """
        append_entries_thread = threading.Thread(target=self.append_entries_loop,
                                               name='append_entries_loop_'+str(self.port))
        append_entries_thread.start()

    
    def append_entries(self, port, timeout):
        data = {'term': self.current_term, 'leader_id': self.port}
        # try to post heartbeat message. it could fail if the node has died. if so, just return unsuccessful
        try:
            return requests.post(f'{LOCALHOST}:{port}{APPEND}', json=data, headers=HEADERS)
        except requests.exceptions.ConnectionError:
            return None


    def process_append_response(self, term, success):
        """ The receiving node is either a leader or a candidate/follower that just changed from a leader. Need to update its current term and change to follower if it learns it's behind """
        if not success:
            self.current_term = term
            self.role = Role.FOLLOWER
            self.election_flag = False
            self.init()


    def append_entries_loop(self):
        """As long as role is leader, launches a pool of threads to append entries to all other nodes using executor for asynchronous execution."""
        if len(self.nodes) > 0:
            while self.role == Role.LEADER:
                sleep(HEARTBEAT)
                with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
                    future_append_entries = {executor.submit(self.append_entries, node, 60): node for node in self.nodes}
                    for future in concurrent.futures.as_completed(future_append_entries):
                        port = future_append_entries[future]
                        data = future.result()
                        if data:
                            self.process_append_response(data.json['term'], data.json['success'])
                            # logging.debug(f'{time.time()} {data.text}')


    def set_term(self, term):
        self.current_term = term
    
    def get_term(self):
        return self.current_term

    def get_role(self):
        if self.role.name == 'LEADER':
            return 'Leader'
        elif self.role.name == 'FOLLOWER':
            return 'Follower'
        return 'Candidate'

    def get_queue(self):
        return self.message_queue