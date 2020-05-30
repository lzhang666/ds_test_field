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

# log_entry = [(term, command, topic, message, commit_bool),...]
log_entries = []

# term is to the best of the node's knowledge
all_ports = set()
ip = None
port = None

role = {'Follower': True, 'Leader':False, 'Candidate': True}

# node state variables
current_term = 0
leaderId = None
candidateId = None
follower_notified = False

# log replication variables
leaderCommit = -1
prevLogIndex = -1
prevLogTerm = 0
entries_to_send = []
commitIndex = -1

nextIndex = {}
matchIndex = {}


# variable to assist get_message function
get_message = None

# variable to track if leader just commited
leader_commited = False 

# follower_reply = {}
#     'currentTerm': int,
#     'votedFor':None,
#     'log':[],
#     'from_port':str,
#     'nextIndex':int
# }

# AppendEntries
# append_entries = {}
    # 'term':int, 
    # 'leaderId':str,
    # 'prevLogIndex':int,
    # 'prevLogTerm':int,
    # 'entries':[],
    # 'leaderCommit':int
    # } 

# RequestVote
request_vote = {}
    # 'term':int,
    # 'candidateId':int,
    # 'lastLogIndex':int,
    # 'lastlogterm':int
    # }

# candidate vote counter
vote_count = {}
headers = {'Content-Type':'application/json'}


def parse_file(file, index):
    """Parse the config.json file given the index of current node
    param: file, index
    return: (ip <str>, port <str>, port_set <list(str)>)
    """
    config = json.load(file)

    ip = config['addresses'][index]['ip'].replace('http://', '')
    port = config['addresses'][index]['port']
    ports_set = set([str(node['port']) for node in config['addresses']])
    return ip, str(port), ports_set


@app.route('/topic', methods=['PUT']) # modify log if toopic created
def put_topic():
    # check leader node condition
    if get_role() != 'Leader':
        return jsonify({'success': False}), 400

    topic = request.json['topic']
    # condition to wait for leader commit
    commit_wait()
    # if topic already exist, return false
    if message_queue.get(topic):
        return jsonify({'success': False}), 400

    # if not already a topic, append to log_entry
    log_entries.append((current_term, 'PUT', topic, None, False))
    print('11'*10, log_entries)

    commit_wait()
    return jsonify({'success': True}), 200


@app.route('/topic', methods=['GET']) 
def get_topic():
    # check leader node condition
    if get_role() != 'Leader':
        return jsonify({'success': False}), 400

    while True:
        if commitIndex == len(log_entries)-1:
            break
    return jsonify({'success': True, 'topics': list(message_queue.keys())}), 210


@app.route('/message', methods=['PUT']) # modify log
def put_message():
    # check leader node condition
    if get_role() != 'Leader':
        return jsonify({'success': False}), 400

    topic = request.json['topic']
    msg = request.json['message']

    commit_wait()
    q = message_queue.get(topic)

    # returns failure if topic does not exist
    if not q:
        return jsonify({'success': False}), 420
    
    # if topic exist, append message to log_entry
    log_entries.append((current_term, 'PUT', topic, msg, False))
    print('22'*10, log_entries)
    
    commit_wait()
    return jsonify({'success': True}), 220



@app.route('/message/<path:path>', methods=['GET']) # modify log if suceess
def get_message(path):
    global get_message
    if get_role() != 'Leader':
        return jsonify({'success': False}), 400
    
    commit_wait()
    q = message_queue.get(path)

    if q == None or q.qsize() == 0:
        return jsonify({'success': False}), 430

    log_entries.append((current_term, 'GET', path, None, False))
    get_message = None
    commit_wait()
    return jsonify({'success': True, 'message': get_message}), 230


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
    return {'role' : get_role(), 'term' : current_term}


@app.route('/leader_channel', methods=['POST'])
def leader_listen():
    recv = request.json
    if current_term == recv['term']:
        sender_port = recv['from_port']
        print(f'receive ack from follower {sender_port} at term {current_term}')

        if len(recv) == 4: 
            follower_next_index = recv['nextIndex']
            if follower_next_index > nextIndex[sender_port]:
                nextIndex[sender_port] = follower_next_index
        # if follower_next_index < nextIndex[sender_port]:
        #     pass # roll back
            # add vote
            vote_count[sender_port] = True

    elif current_term < recv['term']:
        set_role('Follower')
    return jsonify({'success': True}), 201


@app.route('/follower_channel', methods=['POST'])
def follower_listen():
    global current_term, leaderId, candidateId, follower_notified, log_entries, leaderCommit
    recv = request.json
    follower_notified = True
    if 'leaderId' in recv:
        if len(recv) == 2: # heartbeat
            if recv['term'] > current_term:
                current_term = recv['term']
                leaderId = recv['leaderId']
            print('follower {} receive heartbeat from leader {} at term {}'.format(port, recv['leaderId'], recv['term']))
            heartbeat_reply = set_heartbeat_reply
            try:
                response = requests.post(f'http://{ip}:{leaderId}/leader_channel', json=heartbeat_reply, headers=headers)
            except Exception:
                pass
            return jsonify({'success': True,}), 201

        elif len(recv) == 3: # commit success
            if recv['term'] > current_term:
                current_term = recv['term']
                leaderId = recv['leaderId']
            # follower commit change
            follower_attempt_commit(recv)
            commit_heartbeat_reply = set_heartbeat_reply()
            try:
                response = requests.post(f'http://{ip}:{leaderId}/leader_channel', json=commit_heartbeat_reply, headers=headers)
            except Exception:
                pass
            return jsonify({'success': True,}), 201

        else: # append entries
            if recv['term'] > current_term:
                current_term = recv['term']
                leaderId = recv['leaderId']

            print('follower {} receive append entries from leader {} at term {}'.format(port, recv['leaderId'], recv['term']))
            # update follower log
            log_entries.append(recv['entries'])
            append_entries_reply = set_append_entries_reply()
            try:
                response = requests.post(f'http://{ip}:{leaderId}/leader_channel', json=append_entries_reply, headers=headers)
            except Exception:
                pass
            return jsonify({'success': True,}), 201


    elif 'candidateId' in recv:
        # print('candidate ID:', recv['candidateId'])
        if recv['term'] > current_term:
            current_term = recv['term']
            candidateId = recv['candidateId']
            reply_candidate = set_reply_candidate()
        print('receive vote request from candidate {} at term {}'.format(recv['candidateId'], recv['term']))
        try:
            vote_candidate = requests.post(f'http://{ip}:{candidateId}/candidate_channel', json=reply_candidate, headers=headers)
        except Exception:
            pass
        return jsonify({'success': True,}), 201
    return jsonify({'success': False}), 400


@app.route('/candidate_channel', methods=['POST'])
def candidate_listen():
    global current_term
    recv = request.json
    if current_term == recv['term'] and port == recv['votedFor']:
        vote_count[recv['from_port']] = True
    elif current_term < recv['term']:
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
    global current_term, leaderId, candidateId, follower_notified, port
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
    global current_term, candidateId, leaderId, port, commitIndex, leader_commited
    set_role('Leader') # assume leader role
    candidateId = None # reset candidateId
    leaderId = port
    heartbeat_timeout = 0.050

    other_ports = set(all_ports)
    other_ports.discard(port)

    reset_vote_count()
    # reset nextIndex dict
    reset_next_index()
    
    while True:
        # check role, if became follower due to lower term
        # assume follower role
        if get_role == 'Follower':
            follower_state()
            break

        # attempt commit message
        leader_attempt_commit()

        # print(f'leader {leaderId} at term {term} sending heartbeat')
        vote_count[leaderId] = True
        for follower in other_ports:
            try:
                if commitIndex < len(log_entries) - 1:
                    append_entries = set_append_entries()
                    response = requests.post(f'http://{ip}:{follower}/follower_channel', json=append_entries, headers=headers)
                elif leader_commited:
                    commit_heartbeat = set_commit_heartbeat()
                    response = requests.post(f'http://{ip}:{follower}/follower_channel', json=commit_heartbeat, headers=headers)
                    leader_commited = False
                else:
                    heartbeat =  set_heartbeat()
                    response = requests.post(f'http://{ip}:{follower}/follower_channel', json=heartbeat, headers=headers)
            except Exception as e:
                continue
        time.sleep(heartbeat_timeout)


def candidate_state():
    global current_term, candidateId, port, leaderId
    leaderId = None
    set_role('Candidate') # assume candidate role
    current_term += 1 # increment term at candidate start
    election_timeout = set_election_timeout()

    other_ports = set(all_ports)
    other_ports.discard(port)

    reset_vote_count()
    vote_count[port] = True
    request_heartbeat = 0

    while True:
        # send request vote to followers
        if request_heartbeat % 50 == 0:
            for vote_port in other_ports:
                if not vote_count[vote_port]:
                    try:
                        url = f'http://{ip}:{vote_port}/follower_channel'
                        set_request_vote()
                        to_follower = requests.post(url, json=request_vote, headers=headers)
                        print(f'candidate {port} at term {current_term} sending request vote to follower {vote_port}')
                    except Exception as e:
                        print(f'candidate {port} request term {current_term} vote from {vote_port} not sent')
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
            current_term += 1 # increment term at candidate restart
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
    global current_term, port
    request_vote['term'] = current_term
    request_vote['candidateId'] = port


def set_heartbeat():
    global current_term, port
    heartbeat = {
        'term':current_term,
        'leaderId':port
    }
    return heartbeat


def set_heartbeat_reply():
    heartbeat_reply = {
        'term':current_term,
        'leaderId':leaderId,
        'from_port':port
    }
    return heartbeat_reply


def set_append_entries(follower_port):
    global current_term, port, prevLogIndex, prevLogTerm, commitIndex
    append_entries = {}
    follower_nextIndex = nextIndex(follower_port)
    prevLogIndex = follower_nextIndex - 1
    prevLogTerm = log_entries[prevLogIndex][0]

    append_entries['term'] = current_term
    append_entries['leaderId'] = port
    append_entries['entries'] = log_entries[follower_nextIndex]
    append_entries['leaderCommit'] = commitIndex
    append_entries['prevLogIndex'] = prevLogIndex
    append_entries['prevLogTerm'] = prevLogTerm
    return append_entries

def set_append_entries_reply():
    reply = {
        'term':current_term,
        'leaderId':leaderId,
        'from_port':port,
        'nextIndex':len(log_entries)
    }
    return reply

def set_commit_heartbeat():
    commit_heartbeat = {
        'term':current_term,
        'leaderId':port,
        'leaderCommit':commitIndex
    }
    return commit_heartbeat

def set_reply_candidate():
    global current_term, port, candidateId
    reply_candidate = {
        'term': current_term,
        'from_port': port,
        'votedFor': candidateId
    }
    return reply_candidate
    



def update_follower_log(recv):
    global log_entries
    recv_entries = recv['entries']
    recv_prevLogIndex = recv['prevLogIndex']
    if (len(log_entries) - 1) == recv_prevLogIndex:
        log_entries += recv_entries
        #commit log_entries
    elif (len(log_entries) - 1) > recv_prevLogIndex:
        log_entries = log_entries[:recv_prevLogIndex+1] + recv_entries
        #commit 
    else: #(len(log_entries) - 1) < recv_prevLogIndex:
        return log_entries # return to leader for checking
    return []


def leader_attempt_commit():
    '''leader attempts to commit log to message queue
    1. if recv_log = [], the clients has updated their log
        if majority vote received, start commit
    2. if recv_log is not None, client has not update their log, implement later
    '''
    global leaderCommit, commitIndex, leader_commited

    cur_count = get_vote_count()
    if commitIndex < len(log_entries) and cur_count >= (len(all_ports)//2+1):
        for idx in range(commitIndex+1, len(log_entries)):
            update_message_queue(log_entries[idx])
        # update commitIndex
        commitIndex = len(log_entries) - 1
        leaderCommit = commitIndex
        leader_commited = True


def follower_attempt_commit(recv):
    global leaderCommit, commitIndex
    leaderCommit = recv['leaderCommit']
    for idx in range(commitIndex+1, len(log_entries)):
        update_message_queue(log_entries[idx])
    commitIndex = min(leaderCommit, len(log_entries)-1)


def update_message_queue(entry):
    global get_message
    topic = entry[2]
    if entry[1] == 'PUT':
        if entry[3] is None: # put topic
            message_queue[topic] = queue.Queue()
        else: # put message
            msg = entry[3]
            q = message_queue.get(topic)
            q.put(msg)
    else: # 'GET' message, may cause error
        q = message_queue.get(topic)
        get_message = q.get(False)
        print(get_message)
    print(list(message_queue.keys()))


def commit_wait():
    '''use in GET/PUT function to wait for previous log entry committed before query
    '''
    while True:
        if commitIndex == len(log_entries)-1:
            break


def reset_next_index():
    global nextIndex, all_ports
    other_ports = set(all_ports)
    other_ports.discard(port)
    print('y'*10, other_ports)
    for p in other_ports:
        nextIndex[p] = commitIndex + 1


def main():
    global ip, port, all_ports
    ip, port, all_ports = parse()
    node_thread = Thread(target=node_init)
    node_thread.start()
    app.run(host=ip, port=int(port), debug=False, threaded=True)
    

if __name__ == "__main__":
    main()
