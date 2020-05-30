"""Requirements
- PUT(topic, message): append message to end of topic's message queue
- GET(topic): pop the first message from topic's queue and return message
"""
import argparse
import queue
import json
from flask import Flask, request, jsonify
import time
from threading import Thread
import requests
import random,time

app = Flask(__name__)

# implement message queue as a dictionary with topic as key and a queue as value
# message_queue<Topic, Queue>
message_queue = {}

# term is to the best of the node's knowledge
# all_ports = ['8567', '9123', '8889']
all_ports = set()
ip = None
port = None

role = {'Follower': True, 'Leader':False, 'Candidate': True}

# node_info using for leader election communication
# node_info = {'following': None, 'notified': False, 'sender_role':''}

term = 0
leaderId = None
candidateId = None
follower_notified = False
log = []

state = {'currentTerm': 0,
        'votedFor':None,
        'log':[],
        'from_port':0
        }

# AppendEntries
append_entries = {
    'term':0, 
    'leaderId':0,
    'prevLogIndex':0,
    'leaderCommit':0
    } 

request_vote = {}
    # 'term':term,
    # 'candidateId':candidateId,
    # 'lastLogIndex':0,
    # 'lastlogterm':0
    # }

# canadidate vote counter
vote_count = {}

headers = {'Content-Type':'application/json'}


def parse_file(file, index):
    """Parse the config.json file given the index of current node
    param: file, index
    return: (ip <string>, port <int>)
    """
    config = json.load(file)

    ip = config['addresses'][index]['ip'].replace('http://', '')
    port = config['addresses'][index]['port']
    ports_set = set([str(node['port']) for node in config['addresses']])
    return ip, str(port), ports_set


@app.route('/topic', methods=['PUT'])
def put_topic():
    # check leader node condition
    if get_role() == 'Leader':
        topic = request.json['topic']
        # if topic already exist, return false
        if message_queue.get(topic) != None:
            return jsonify({'success': False}), 400

        # if not already a topic, make the value an empty queue
        message_queue[topic] = queue.Queue()
        return jsonify({'success': True}), 200

    return jsonify({'success': False}), 400


@app.route('/topic', methods=['GET'])
def get_topic():
    # check leader node condition
    if get_role() == 'Leader':
        return jsonify({'success': True, 'topics': list(message_queue.keys())}), 210
    return jsonify({'success': False}), 400


@app.route('/message', methods=['PUT'])
def put_message():
    # check leader node condition
    if get_role() == 'Leader':
        topic = request.json['topic']
        msg = request.json['message']

        q = message_queue.get(topic)

        # returns failure if topic does not exist
        if q == None:
            return jsonify({'success': False}), 420
        
        q.put(msg)
        return jsonify({'success': True}), 220
    return jsonify({'success': False}), 400


@app.route('/message/<path:path>', methods=['GET'])
def get_message(path):
    if get_role() == 'Leader':
        q = message_queue.get(path)
        if q == None or q.qsize() == 0:
            return jsonify({'success': False}), 430

        # implementing this as try except block because subsequent calls to empty() and get() can result in unexpected behavior
        # if not empty before, it can become empty immediately afterwards
        # q.get(False) returns an exception if queue is empty 
        # try:
        return jsonify({'success': True, 'message': q.get(False)}), 230
        # except queue.Empty:
        #     return jsonify({'success': False}), 431
    return jsonify({'success': False}), 400


def parse():
    """parse argument from comment line
    """
    parser = argparse.ArgumentParser(description='Start a new node.',
                                     prog='node',
                                     usage='%(prog)s <path_to_config> <index>')

    parser.add_argument('path_to_config', help='JSON file with list of ip and port of all nodes in RRMQ')
    parser.add_argument('index', type=int, help='index of current server in JSON list of addresses that will be used to let server know its own IP and port')

    args = parser.parse_args()

    path = args.path_to_config
    index = args.index

    with open(path) as f:
        ip, port, ports_list = parse_file(f, index)
        print(ip, port, ports_list)

    return ip, port, ports_list

@app.route('/status', methods=['GET'])
def get_status():
    return {'role' : get_role(), 'term' : term}


@app.route('/leader_channel', methods=['POST'])
def leader_listen():
    recv = request.json
    if term == recv['currentTerm']:
        sender_port = recv['from_port']
        print(f'receive ack from follower {sender_port} at term {term}')
    elif term < recv['currentTerm']:
        set_role('Follower')
    return jsonify({'success': True}), 201


@app.route('/follower_channel', methods=['POST'])
def follower_listen():
    global term, leaderId, candidateId, follower_notified
    recv = request.json
    follower_notified = True
    if 'leaderId' in recv:
        # if received term > node term, update term and leader
        if recv['term'] > term:
            term = recv['term']
            leaderId = recv['leaderId']
            set_state('Leader')
        print('receive heartbeat from leader {} at term {}'.format(recv['leaderId'], recv['term']))
        try:
            notify_leader = requests.post(f'http://127.0.0.1:{leaderId}/leader_channel', json=state, headers=headers)
        except Exception:
            pass
        return jsonify({'success': True,}), 201
    elif 'candidateId' in recv:
        # print('candidate ID:', recv['candidateId'])
        if recv['term'] > term:
            term = recv['term']
            candidateId = recv['candidateId']
            set_state('Candidate')
        print('receive vote request from candidate {} at term {}'.format(recv['candidateId'], recv['term']))
        
        try:
            vote_candidate = requests.post(f'http://127.0.0.1:{candidateId}/candidate_channel', json=state, headers=headers)
        except Exception:
            pass
        return jsonify({'success': True,}), 201
    return jsonify({'success': False}), 400


@app.route('/candidate_channel', methods=['POST'])
def candidate_listen():
    global term
    recv = request.json
    if term == recv['currentTerm'] and port == recv['votedFor']:
        print('candidate/follower channel', recv['from_port'])
        vote_count[recv['from_port']] = True
    elif term < recv['currentTerm']:
        pass # here assume follower role again 
    return jsonify({'success': True}), 201


def node_init():
    """start node as a follower
    """
    follower_state()
    

def set_election_timeout():
    num = random.randint(150,300)
    return num


def follower_state():
    global term, leaderId, candidateId, follower_notified, port
    election_timeout = set_election_timeout()
    set_role('Follower')
    
    while True:
        if follower_notified:
            election_timeout = set_election_timeout()
            follower_notified = False # reset follower_notified for next iteration
        if election_timeout == 0:
            candidate_state()
            break
        time.sleep(0.001)
        election_timeout -= 1
            

def leader_state():
    """
    heartbeat timeout every 0.050 ms
    send all the nodes leader ack
    """
    # set_leader_state()
    global term, candidateId, leaderId, port
    set_role('Leader') # assume leader role
    candidateId = None # reset candidateId
    leaderId = port
    heartbeat_timeout = 0.050
    comm_ports = set(all_ports)
    comm_ports.discard(port)

    while True:
        if get_role == 'Follower':
            follower_state()
            break

        print(f'leader {leaderId} at term {term} sending heartbeat')
        for follower in comm_ports:
            try:
                set_append_entries()
                send_msg = requests.post(f'http://127.0.0.1:{follower}/follower_channel', json=append_entries, headers=headers)
            except Exception as e:
                continue
        time.sleep(heartbeat_timeout)


def candidate_state():
    global term, candidateId, port, leaderId
    leaderId = None
    set_role('Candidate') # assume candidate role
    term += 1 # increment term at candidate start
    election_timeout = set_election_timeout()

    comm_ports = set(all_ports)
    comm_ports.discard(port)

    reset_vote_count()
    vote_count[port] = True
    request_heartbeat = 0
    while True:
        # send request vote to followers
        if request_heartbeat % 50 == 0:
            for vote_port in vote_count:
                if not vote_count[vote_port] and vote_count[vote_port] != port:
                    try:
                        url = f'http://127.0.0.1:{vote_port}/follower_channel'
                        set_request_vote()
                        # print(request_vote)
                        to_follower = requests.post(url, json=request_vote, headers=headers)
                        print(f'candidate {port} at term {term} sending request vote to follower {vote_port}')
                    except Exception as e:
                        print(f'candidate {port} request term {term} vote from {vote_port} not sent')
                        continue
        
        # check vote count
        cur_count = get_vote_count()
        
        # if vote count valid, assume leader role
        if cur_count >= len(all_ports)//2+1:
            reset_vote_count()
            leader_state()
            break
        
        # if receiver a leader ack, assume follower role
        if leaderId:
            reset_vote_count()
            set_role('Follower') # assume follower state
            follower_state()

        # if timeout, stay as candidate and restart request vote
        if election_timeout == 0:
            election_timeout = set_election_timeout()
            term += 1 # increment term at candidate restart
        time.sleep(0.001)
        request_heartbeat += 1
        election_timeout -= 1


def set_role(to_role):
    for key in role:
        role[key] = False
    role[to_role] = True


def get_role():
    for key in role:
        if role[key]:
            return key


def set_role(to_role):
    for key in role:
        role[key] = False
    role[to_role] = True


def reset_vote_count():
    for port in all_ports:
        vote_count[port] = False


def get_vote_count():
    cur_count = 0
    for vote in vote_count:
            if vote_count[vote]:
                cur_count += 1
    return cur_count

def set_request_vote():
    global term, port
    request_vote['term'] = term
    request_vote['candidateId'] = port

def set_append_entries():
    global term, port
    append_entries['term'] = term
    append_entries['leaderId'] = port

def set_state(to_role):
    global term, port, candidateId, state, leaderId
    state['currentTerm'] = term
    state['from_port'] = port
    if to_role == 'Candidate':
        state['votedFor'] = candidateId
    elif to_role == 'Leader':
        state['votedFor'] = None


def main():
    global ip, port, all_ports
    ip, port, all_ports = parse()
    node_thread = Thread(target=node_init)
    node_thread.start()
    app.run(host=ip, port=int(port), debug=False, threaded=True)
    
if __name__ == "__main__":
    main()

 

