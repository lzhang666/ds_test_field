"""Requirements:
1. Raft election protocol
2. Only leader replies to client PUT/GET of messages/topics. If not leader, respond failure. Status works for all nodes.
3. Add timeout capability so it can detect leadre failure.
4. Heartbeat message (using AppendEntries) so leader can notify other nodes of life.
5. Test implementation by removing leaders and validating new leader.
"""

import argparse
import queue
import json
import threading

from state import State
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/topic', methods=['PUT'])
def put_topic():
    if not state.get_role() == 'Leader':
        return jsonify({'success': False}), 401

    topic = request.json['topic']

    """ Kevin - What are some other instances in which creating a topic could fail? """
    # if topic already exist, return false
    if state.get_queue().get(topic) != None:
        return jsonify({'success': False}), 400

    # if not already a topic, make the value an empty queue
    state.get_queue()[topic] = queue.Queue()
    return jsonify({'success': True}), 200


@app.route('/topic', methods=['GET'])
def get_topic():
    if not state.get_role() == 'Leader':
        return jsonify({'success': False}), 411
    """ Kevin - How can this fail? Interface says to return empty list if no topics. """
    return jsonify({'success': True, 'topics': list(state.get_queue().keys())}), 210


@app.route('/message', methods=['PUT'])
def put_message():
    if not state.get_role() == 'Leader':
        return jsonify({'success': False}), 421

    topic = request.json['topic']
    msg = request.json['message']

    q = state.get_queue().get(topic)

    # returns failure if topic does not exist
    if q == None:
        return jsonify({'success': False}), 420
    
    q.put(msg)
    return jsonify({'success': True}), 220


@app.route('/message/<path:path>', methods=['GET'])
def get_message(path):
    if not state.get_role() == 'Leader':
        return jsonify({'success': False}), 431

    q = state.get_queue().get(path)
    if q == None:
        return jsonify({'success': False,}), 430

    # implementing this as try except block because subsequent calls to empty() and get() can result in unexpected behavior
    # if not empty before, it can become empty immediately afterwards
    # q.get(False) returns an exception if queue is empty 
    try:
        return jsonify({'success': True, 'message': q.get(False)}), 230
    except queue.Empty:
        return jsonify({'success': False}), 431


@app.route('/status', methods=['GET'])
def get_status():
    """Used for testing the leader election algorithm
    return: role<String> and term<int> as JSON 
    """
    return jsonify({'role': state.get_role(), 'term': state.get_term()})


@app.route('/append/<path:path>', methods=['GET'])
def get_entry(path):
    """Get the entry from the queue. Called by the client"""
    pass


@app.route('/append', methods=['POST'])
def append_entries():
    """Put an entry into the queue. Called by the client and leader node. Also acts as heartbeat."""
    term = request.json['term']
    term, success = state.respond_append(term)
    return jsonify({'term': term, 'success': success})


# @app.route('/elect/', methods=['POST'])
# def put_election(path):
#     """Forward election results to other nodes. Called by the LEADER node after successful election"""
#     pass


@app.route('/election', methods=['POST'])
def request_vote():
    """Respond to a vote request from candidates"""
    term = request.json['term']
    candidate_id = request.json['candidate_id']
    term, vote_granted = state.respond_vote(term)
    return jsonify({'term': term, 'vote_granted': vote_granted})


# @app.route('/heartbeat', methods=['POST'])
# def heartbeat():
#     leader_id = request.json['term']
#     alive = request.json['leader_id']
#     return jsonify({'leader_id': leader_id, 'status': 'received'})


def parse_file(file, index):
        """Parse the config.json file given the index of current node
        param: file, index
        return: (ip <string>, port <int>)
        """
        config = json.load(file)

        ip = config['addresses'][index]['ip'].replace('http://', '')
        port = config['addresses'][index]['port']
        nodes = set([node['port'] for node in config['addresses'] if node['port'] != port])

        return ip, port, nodes

def run(ip, port, nodes):
    app.run(host=ip, port=port, debug=False, threaded=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Start a new node.',
                                     prog='node',
                                     usage='%(prog)s <path_to_config> <index>')

    parser.add_argument('path_to_config', help='JSON file with list of ip and port of all nodes in RRMQ')
    parser.add_argument('index', type=int, help='index of current server in JSON list of addresses that will be used to let server know its own IP and port')

    args = parser.parse_args()

    path = args.path_to_config
    index = args.index

    with open(path) as f:
        ip, port, nodes = parse_file(f, index)

    run_thread = threading.Thread(target=run,
                                  args=(ip, port, nodes),
                                  name=str(port)+'_entries')
    run_thread.start()
    state = State(ip, port, nodes)