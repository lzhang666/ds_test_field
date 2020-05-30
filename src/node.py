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

# log_entry = [(term, command, topic, message),...]
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
commitIndex = -1

nextIndex = {} # for each server, index of the next log entry to send to that server

# counting votes for commit {log_index:vote_num}
leader_vote_cnt = {}

# global variable to assist get_message function
get_message = None

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
    log_entries.append((current_term, 'PUT', topic, None))
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
    log_entries.append((current_term, 'PUT', topic, msg))
    # print('22'*10, log_entries)
    
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

    log_entries.append((current_term, 'GET', path, None))
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
    global leader_vote_cnt
    recv = request.json
    if current_term == recv['term']:
        sender_port = recv['from_port']
        # print(f'receive ack from follower {sender_port} at term {current_term}')

        if len(recv) == 4: 
            # print('!!!!!!!!!!!!!!!!leader receive commit vote!!!!!!!!!!')
            # print('leader receiver reply to append_entries from follower')
            flw_nx_idx = recv['nextIndex']
            # print('In leader listen 1', nextIndex)
            if flw_nx_idx > nextIndex[sender_port]:
                nextIndex[sender_port] = flw_nx_idx
            

            # add vote leader_vote_cnt
            if (flw_nx_idx - 1) in leader_vote_cnt:
                leader_vote_cnt[flw_nx_idx - 1] += 1
            else:
                leader_vote_cnt[flw_nx_idx - 1] = 1

            
            print('updated vote count', leader_vote_cnt)
            print('In leader listen 2', nextIndex)
    elif current_term < recv['term']:
        print('leader change to follower')
        set_role('Follower')
    return jsonify({'success': True}), 201


@app.route('/follower_channel', methods=['POST'])
def follower_listen():
    global current_term, leaderId, candidateId, follower_notified, log_entries, leaderCommit
    recv = request.json
    follower_notified = True
    if 'leaderId' in recv:
        # print('processing message from leader')
        if recv['term'] >= current_term:
                current_term = recv['term']
                leaderId = recv['leaderId']

        if len(recv) == 3: # regular heartbeat or commit heartbeat
            if recv['leaderCommit'] > leaderCommit:
                leaderCommit = recv['leaderCommit']
                follower_attempt_commit()
            heartbeat_reply = set_heartbeat_reply()
            # print(heartbeat_reply)
            try:
                response = requests.post(f'http://{ip}:{leaderId}/leader_channel', json=heartbeat_reply, headers=headers)
            except Exception:
                print('not sent2')
                pass
            return jsonify({'success': True,}), 201

        else: # append entries sent from leader
            print('follower {} receive append entries from leader {} at term {}'.format(port, recv['leaderId'], recv['term']))
            # update follower log
            print('print received entry', recv['entries'])
            if recv['prevLogIndex'] == len(log_entries) - 1:
                log_entries.append(recv['entries'])
            # print('log_entries after append', log_entries)
            append_entries_reply = set_append_entries_reply()
            # print(append_entries_reply)
            try:
                response = requests.post(f'http://{ip}:{leaderId}/leader_channel', json=append_entries_reply, headers=headers)
            except Exception:
                print('not sent3')
                pass
            return jsonify({'success': True,}), 201


    elif 'candidateId' in recv:
        # print('candidate ID:', recv['candidateId'])
        print('processing message from candidate')
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
    


@app.route('/candidate_channel', methods=['POST'])
def candidate_listen():
    global current_term
    recv = request.json
    if current_term == recv['term'] and port == recv['votedFor']:
        vote_count[recv['from_port']] = True
    elif current_term < recv['term']:
        print('candidate change to follower')
        set_role('Follower')
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
    
    print('follower {} at term {}'.format(port, current_term))

    while True:
        # print(list(message_queue.keys()))
        if follower_notified:
            print('follower commitIndex', commitIndex)
            print('follower log latest index', len(log_entries)-1)
            print('follower log entries', log_entries)
            election_timeout = set_election_timeout()
            follower_notified = False # reset follower_notified for next iteration
        if election_timeout == 0:
            print('follower timed out')
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
    global current_term, candidateId, leaderId, port, commitIndex, nextIndex
    set_role('Leader') # assume leader role
    candidateId = None # reset candidateId
    leaderId = port
    heartbeat_timeout = 0.050

    other_ports = set(all_ports)
    other_ports.discard(port)

    # reset nextIndex dict
    reset_next_index()

    print('leader {} elected at term {}'.format(port, current_term))
    print('next index test again', nextIndex)

    while True:
        # check role, if node became follower due to lower term
        # assume follower role
        if get_role == 'Follower':
            print('leader switch to follower')
            follower_state()
            break

        # attempt commit message
        leader_attempt_commit()

        print('leader commitIndex', commitIndex)
        print('leader log latest index', len(log_entries)-1)

        # print(f'leader {leaderId} at term {term} sending heartbeat')
        for follower in other_ports:
            # send communication to follower in threads
            thread = Thread(target=leader_to_others, args=(follower,))
            thread.start()
        time.sleep(heartbeat_timeout)


def leader_to_others(follower):
    if nextIndex[follower] <= len(log_entries) - 1: # append entries
        # print('leader sends append entries to {}'.format(follower))
        append_entries = set_append_entries(follower, nextIndex[follower])
        # print(append_entries)
        try:
            response = requests.post(f'http://{ip}:{follower}/follower_channel', json=append_entries, headers=headers)
            print('next index test again again', nextIndex)
        except Exception as e:
            pass

    else: # heartbeat, including the leader commit info
        # print('leader sends heartbeat')
        heartbeat = set_heartbeat()
        # print(heartbeat)
        try:
            response = requests.post(f'http://{ip}:{follower}/follower_channel', json=heartbeat, headers=headers)
        except Exception as e:
            pass



def candidate_state():
    global current_term, candidateId, port, leaderId
    leaderId = None
    candidateId = port
    set_role('Candidate') # assume candidate role
    current_term += 1 # increment term at candidate start
    election_timeout = set_election_timeout()

    other_ports = set(all_ports)
    other_ports.discard(port)

    reset_election_vote_count()
    vote_count[port] = True
    request_heartbeat = 0

    print('become candidate {} at term {}'.format(port, current_term))

    while True:
        # check role, if node became follower due to lower term
        # assume follower role
        if get_role == 'Follower':
            print('candidate switch to follower')
            follower_state()
            break

        # if receiver a leader ack, assume follower role
        if leaderId:
            reset_election_vote_count()
            set_role('Follower') # assume follower state
            follower_state()

        # send request vote to followers
        if request_heartbeat % 50 == 0:
            for vote_port in other_ports:
                thread = Thread(target=candidate_to_others, args=(vote_port,))
                thread.start()
        
        # check vote count
        cur_count = get_election_vote_count()
        
        # if vote count valid, assume leader role
        if cur_count >= len(all_ports)//2+1:
            reset_election_vote_count()
            leader_state()
            break

        # if timeout, stay as candidate and restart request vote
        if election_timeout == 0:
            election_timeout = set_election_timeout()
            current_term += 1 # increment term at candidate restart
        time.sleep(0.001)
        request_heartbeat += 1
        election_timeout -= 1


def candidate_to_others(vote_port):
    if not vote_count[vote_port]:
        try:
            url = f'http://{ip}:{vote_port}/follower_channel'
            request_vote = set_request_vote()
            to_follower = requests.post(url, json=request_vote, headers=headers)
            # print(f'candidate {port} at term {current_term} sending request vote to follower {vote_port}')
        except Exception as e:
            # print(f'candidate {port} request term {current_term} vote from {vote_port} not sent')
            pass


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


def reset_election_vote_count():
    for port in all_ports:
        vote_count[port] = False


def get_election_vote_count():
    cur_count = 0
    for vote in vote_count:
            if vote_count[vote]:
                cur_count += 1
    return cur_count

def set_request_vote():
    global current_term, port
    request_vote = {
        'term': current_term,
        'candidateId': port
    }
    return request_vote


def set_heartbeat():
    global current_term, port, commitIndex
    heartbeat = {
        'term':current_term,
        'leaderId':port,
        'leaderCommit':commitIndex
    }
    return heartbeat


def set_heartbeat_reply():
    global current_term, leaderId, port
    heartbeat_reply = {
        'term':current_term,
        'leaderId':leaderId,
        'from_port':port
    }
    return heartbeat_reply


def set_append_entries(follower_port, nx_idx):
    global current_term, port, prevLogIndex, prevLogTerm, commitIndex, nextIndex
    prevLogIndex = nx_idx - 1
    prevLogTerm = log_entries[prevLogIndex][0]
    append_entries = {
        'term': current_term,
        'leaderId': port,
        'entries': log_entries[nx_idx],
        'leaderCommit': commitIndex,
        'prevLogIndex': prevLogIndex,
        'prevLogTerm': prevLogTerm
    }
    return append_entries

def set_append_entries_reply():
    reply = {
        'term':current_term,
        'leaderId':leaderId,
        'from_port':port,
        'nextIndex':len(log_entries)
    }
    return reply


def set_reply_candidate():
    global current_term, port, candidateId
    reply_candidate = {
        'term': current_term,
        'from_port': port,
        'votedFor': candidateId
    }
    return reply_candidate


def leader_attempt_commit():
    global leaderCommit, commitIndex, leader_vote_cnt

    other_votes = leader_vote_cnt[commitIndex + 1] if (commitIndex + 1) in leader_vote_cnt else 0
    cur_vote = other_votes + 1
    print('show vote count {} out of {} to update log index {}'.format(cur_vote, len(all_ports), commitIndex+1))
    if commitIndex < len(log_entries)-1 and cur_vote >= (len(all_ports)//2+1):
        print('leader committing'*5)
        # for idx in range(commitIndex+1, len(log_entries)):
        update_message_queue(log_entries[-1])
        # update commitIndex
        commitIndex = len(log_entries)-1
        leaderCommit = commitIndex


def follower_attempt_commit():
    global leaderCommit, commitIndex
    update_message_queue(log_entries[leaderCommit])
    commitIndex += 1


def update_message_queue(entry):
    global get_message, message_queue
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
    print('message queue commited:', list(message_queue.keys()))


def commit_wait():
    '''use in GET/PUT function to wait for previous log entry committed before query
    '''
    while True:
        # print('commitIndex', commitIndex)
        # print('log entries latest index', len(log_entries)-1)
        if commitIndex == len(log_entries)-1:
            break


def reset_next_index():
    global nextIndex, all_ports
    nextIndex = {}
    other_ports = set(all_ports)
    other_ports.discard(port)
    print('y'*10, other_ports)
    print('last log index', len(log_entries))
    for p in other_ports:
        nextIndex[p] = len(log_entries)
    print('next index', nextIndex)


def main():
    global ip, port, all_ports
    ip, port, all_ports = parse()
    node_thread = Thread(target=node_init)
    node_thread.start()
    app.run(host=ip, port=int(port), debug=False, threaded=True)
    

if __name__ == "__main__":
    main()
