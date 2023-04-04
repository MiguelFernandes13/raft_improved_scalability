#!/usr/bin/env python

# Simple 'echo' workload in Python for Maelstrom

#run with
# ./maelstrom test --bin ../uni/4ano/sd/tf/guiao3/lin_kv.py --time-limit 10 --node-count 4 -w lin-kv --concurrency 8n


#Log replication
#1.The leader appends the command to its log as a new entry
#2.The leader sends AppendEntries RPCs to each of the other servers to replicate the entry
#3.When the entry has been safely replicated, the leader increments commitIndex and applies the command to its state machine
#4.Send response to client
#5 If followers crash, leader retries AppendEntries RPCs indefinitely

#Each log entry stores a state machine command along with the term
#number when the entry was received by the leade
import logging
import random
from threading import Thread, Timer, Lock
import time
from ms import receiveAll, reply, exitOnError, send

import enum

class State(enum.Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

logging.getLogger().setLevel(logging.DEBUG)
linkv = {}

state = None
currentTerm = 0
votedFor = None
log = []

prevLogIndex = 0
prevLogTerm = 0

last_index = 1
index = 1
commitIndex = 0
lastApplied = 0

nextIndex = []
matchIndex = []

vote_count = 0

lock_index = Lock()
#Timer
heartbeat_timeout = None
election_timeout = None

class Command:
    def __init__(self, type, key, value, term, index, src, resp=0):
        self.type = type
        self.key = key
        self.value = value
        self.term = term
        self.index = index
        self.src = src
        self.resp = resp

def broadcast(**kwds):
    for i in node_ids:
        if i != node_id:
            send(node_id, i, **kwds)

def request_vote():
    global state, currentTerm, vote_count, votedFor, lastLogIndex, lastLogTerm, election_timeout
    state = State.CANDIDATE
    currentTerm += 1
    vote_count = 1
    votedFor = node_id
    if len(log) == 0:
        lastLogIndex = 0
        lastLogTerm = 0
    else:
        lastLogIndex = len(log) - 1
        lastLogTerm = log[lastLogIndex].term
    logging.debug('request vote')
    broadcast(type='request_vote', term=currentTerm, candidate_id=node_id, lastLogIndex= lastLogIndex, lastLogTerm=lastLogTerm)
    election_timeout = Timer(0.3, reset_election)
    election_timeout.start()

def wait_for_heartbeat():
    global heartbeat_timeout
    random_timeout = random.randint(150, 300)
    logging.debug('wait for heartbeat %d', random_timeout)
    heartbeat_timeout = Timer(random_timeout/1000, request_vote)
    heartbeat_timeout.start()

def send_heartbeat():
    global last_index, prevLogIndex, prevLogTerm, commitIndex, state
    while state == State.LEADER:
        if index > last_index:
            broadcast(type='log_replication', term=currentTerm, leader_id=node_id,
                        prevLogIndex=prevLogIndex, prevLogTerm = prevLogTerm,
                        entries=log[(last_index-1):], leadercommit=commitIndex)
            last_index = index
            prevLogIndex = index - 1
            prevLogTerm = currentTerm
            commitIndex += 1
        else:
            broadcast(type='heartbeat', term=currentTerm, leader_id=node_id,
                        prevLogIndex=prevLogIndex, prevLogTerm = prevLogTerm,
                        entries=[], leadercommit=commitIndex)
        time.sleep(0.07)

def reset_election():
    global state, vote_count, votedFor, currentTerm
    state = State.FOLLOWER
    vote_count = 0
    votedFor = None
    currentTerm -=1
    wait_for_heartbeat()


# Reply false if term < currentTerm
def verify_term(msg):
    global currentTerm
    if msg.body.term < currentTerm:
        return False
    return True

#Reply false if log doesn’t contain an entry at prevLogIndex
#whose term matches prevLogTerm
def verify_prevLogIndex(msg):
    global log
    if len(log) == 0:
        return True
    elif (msg.body.prevLogIndex - 1) < len(log):
        if log[(msg.body.prevLogIndex - 1)].term == msg.body.prevLogTerm:
            return True
    return False

def verify_heartbeat(msg):
    global commitIndex, log
    if verify_term(msg) and verify_prevLogIndex(msg):
        # If an existing entry conflicts with a new one (same index
        #but different terms), delete the existing entry and all that
        #follow it 
        for i in range(msg.body.prevLogIndex + 1, len(log)):
            if i < len(log) and log[i].term != msg.body.term:
                log = log[:i]
                break
        # Append any new entries not already in the log
        for entry in msg.body.entries:
            if entry not in log:
                log.append(entry)
        #If leaderCommit > commitIndex, set commitIndex =
        #min(leaderCommit, index of last new entry)
        if msg.body.leadercommit > commitIndex:
            commitIndex = min(msg.body.leadercommit, len(log)-1)
        return True
    return False

def send_error_not_leader(msg):
    msg.body.code = '11'
    msg.body.text = 'Not leader'
    reply(msg, type='error')
    
for msg in receiveAll():
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)
        state = State.FOLLOWER
        wait_for_heartbeat()

        reply(msg, type='init_ok')
    elif msg.body.type == 'read':
        if state == State.LEADER:
            if msg.body.key in linkv:
                logging.info('read %s', msg.body.key)
                reply(msg, type='read_ok', value=linkv[msg.body.key])

            else:
                msg.body.code = '20'
                msg.body.text = 'Key not found'
                reply(msg, type='error')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            send_error_not_leader(msg)

    elif msg.body.type == 'write':
        logging.info('write %s', msg.body.key)
        if state == State.LEADER:
            c = Command('write', msg.body.key, msg.body.value, currentTerm, index, msg.src)
            index += 1
            log.append(c)       
        else:
            send_error_not_leader(msg)
        
    elif msg.body.type == 'cas':
        logging.info('cas %s', msg.body.key)
        if state == State.LEADER:
            if msg.body.key in linkv:
                if msg.body.value == linkv[msg.body.key]:
                    c = Command('write', msg.body.key, msg.body.value, currentTerm, index, msg.src)
                    index += 1
                    log.append(c)
                else:
                    msg.body.code = '22'
                    msg.body.text = 'From value does not match key'
                    reply(msg, type='error')
                    logging.warning('from does not match value for key %s', msg.body.key)
            else:
                msg.body.code = '21'
                msg.body.text = 'Key not found'
                reply(msg, type='error')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            send_error_not_leader(msg)

    elif msg.body.type == 'log_replication':
        logging.info('log replication')
        # another server establishes itself as leader
        if state == State.CANDIDATE:
            election_timeout.cancel()
            state = State.FOLLOWER
            votedFor = None
        
        heartbeat_timeout.cancel()

        if verify_heartbeat(msg):
            reply(msg, type='log_replication_resp', success=True, term=currentTerm)
            for entry in msg.body.entries:
                if entry.type == 'update':
                    linkv[entry.key] = entry.value
        else:
            reply(msg, type='log_replication_resp', success=False, term=currentTerm)
        wait_for_heartbeat()


    elif msg.body.type == 'log_replication_resp':
        logging.info('log replication response')
        if msg.body.success == True:
            request = log[len(log)-1]
            request.resp += 1
            #If there a majority of replicas responses, apply the command to the state machine
            if request.resp > (len(node_ids)//2) - 1:
                linkv[request.key] = request.value
                send(node_id, request.src, type='write_ok')
                c = Command('update', request.key, request.value, currentTerm, lastApplied, None)
                index += 1
                log.append(c)
                lastApplied += 1
                request.resp = 0

    elif msg.body.type == 'request_vote':
        logging.info('request vote')
        if state != State.LEADER:
            heartbeat_timeout.cancel()
        #Reply false if term < currentTerm
        if verify_term(msg) and (votedFor == None or votedFor == msg.body.candidate_id) and msg.body.lastLogIndex >= len(log)-1:
            votedFor = msg.body.candidate_id
            currentTerm = msg.body.term
            reply(msg, type='request_vote_resp', vote_granted=True, term=currentTerm)
        else:
            reply(msg, type='request_vote_resp', vote_granted=False, term=currentTerm)
        if state != State.LEADER:
            wait_for_heartbeat()


    elif msg.body.type == 'request_vote_resp':
        logging.info('request vote response')
        if msg.body.vote_granted == True:
            vote_count += 1
            if vote_count > (len(node_ids)//2):
                election_timeout.cancel()
                state = State.LEADER
                votedFor = None
                vote_count = 0
                Thread(target=send_heartbeat).start()
                logging.info('leader elected')

    elif msg.body.type == 'heartbeat':
        logging.info('heartbeat')
        if state == State.CANDIDATE:
            election_timeout.cancel()
            state = State.FOLLOWER
            votedFor = None
        heartbeat_timeout.cancel()
        wait_for_heartbeat()