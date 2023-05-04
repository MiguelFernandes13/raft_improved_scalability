#!/usr/bin/env python

# https://raft.github.io/raft.pdf

#run with
# ./maelstrom test --bin ../uni/4ano/sd/tf/raft_improved_scalability/src/lin_kv.py --time-limit 10 --node-count 4 -w lin-kv --concurrency 8n


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

commitIndex = 0
lastApplied = 0

nextIndex = {}
matchIndex = {}

vote_count = 0

lock_index = Lock()
#Timers
heartbeat_timeout = None
election_timeout = None

class Command:
    def __init__(self, type, key, value, term, index, src, resp=1):
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
    global state, currentTerm, vote_count, votedFor, election_timeout, log
    if state != State.LEADER:
        logging.info('request vote')
        state = State.CANDIDATE
        currentTerm += 1
        vote_count = 1
        votedFor = node_id
        if len(log) == 0:
            lastLogIndex = 0
            lastLogTerm = 0
        else:
            lastLogIndex = len(log)
            lastLogTerm = log[lastLogIndex - 1].term
        broadcast(type='request_vote', term=currentTerm, candidate_id=node_id, lastLogIndex= lastLogIndex, lastLogTerm=lastLogTerm)
        random_timeout = random.randint(150, 300)
        election_timeout = Timer(random_timeout/1000, request_vote)
        election_timeout.start()

def wait_for_heartbeat():
    global heartbeat_timeout
    random_timeout = random.randint(150, 300)
    heartbeat_timeout = Timer(random_timeout/1000, request_vote)
    heartbeat_timeout.start()

def send_heartbeat():
    global commitIndex, state, log, currentTerm
    while state == State.LEADER:
        for i in node_ids:
            if i != node_id:
                if len(log) == 0 or nextIndex[i] == 0:
                    prevLogIndex = 0
                    prevLogTerm = 0
                else:
                    prevLogIndex = nextIndex[i]
                    prevLogTerm = log[prevLogIndex - 1].term
                logging.info('nextIndex: ' + str(nextIndex[i]) + ' len(log): ' + str(len(log)))
                if nextIndex[i] < len(log):
                    logging.info('sending log_replication with entries')
                    send(node_id, i, type='log_replication', term=currentTerm,
                            leader_id=node_id, prevLogIndex=prevLogIndex,
                            prevLogTerm = prevLogTerm, entries=log[nextIndex[i]:],
                            leadercommit=commitIndex)
                else:
                    logging.info('sending log_replication without entries')
                    send(node_id, i, type='log_replication', term=currentTerm, leader_id=node_id,
                            prevLogIndex=prevLogIndex, prevLogTerm = prevLogTerm, entries=[], leadercommit=commitIndex)
        time.sleep(0.07)

# Reply false if term < currentTerm
def verify_term(msg):
    global currentTerm, votedFor, state
    if msg.body.term >= currentTerm:
        currentTerm = msg.body.term
        if state != State.FOLLOWER:
            if state == State.CANDIDATE:
                logging.info('cancel new election timeout')
                election_timeout.cancel()
            state = State.FOLLOWER
            votedFor = None
        return True
    elif msg.body.term < currentTerm:
        return False

#Reply false if log doesn’t contain an entry at prevLogIndex
#whose term matches prevLogTerm
def verify_prevLogIndex(msg):
    global log
    if len(log) == 0 and msg.body.prevLogIndex == 0:
        return True
    elif (msg.body.prevLogIndex - 1) < len(log):
        if log[(msg.body.prevLogIndex - 1)].term == msg.body.prevLogTerm:
            return True
    return False

def verify_heartbeat(msg):
    global commitIndex, log, state, heartbeat_timeout, election_timeout, currentTerm, votedFor, lastApplied
    if verify_term(msg):
        logging.info('term verified')
        #reset heartbeat timeout
        heartbeat_timeout.cancel()
       
        if verify_prevLogIndex(msg):
            logging.info('prevLogIndex verified')
            # If an existing entry conflicts with a new one (same index
            #but different terms), delete the existing entry and all that
            #follow it 
            for i in range(msg.body.prevLogIndex + 1, len(log)):
                if i < len(log) and log[i].term != msg.body.term:
                    log = log[:i]
                    break
            # Append any new entries not already in the log
            for entry in msg.body.entries:
                if entry.index > len(log):
                    log.append(entry)
            #If leaderCommit > commitIndex, set commitIndex =
            #min(leaderCommit, index of last new entry)
            if msg.body.leadercommit > commitIndex:
                logging.info('commitIndex updated from %s to %s', commitIndex, min(msg.body.leadercommit, len(log)))
                commitIndex = min(msg.body.leadercommit, len(log))
                for i in range(lastApplied, commitIndex ):
                    logging.info('commit %s %s', log[i].key, log[i].value)
                    linkv[log[i].key] = log[i].value
                lastApplied = commitIndex
            return True
    return False


#def send_error_not_leader(msg):
#    msg.body.code = '11'
#    msg.body.text = 'Not leader'
#    reply(msg, type='error')
    
def send_error(msg, code, text):
    msg.body.code = code
    msg.body.text = text
    reply(msg, type='error')
    
for msg in receiveAll():
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        state = State.FOLLOWER
        for i in node_ids:
            nextIndex[i] = 0
            matchIndex[i] = 0
        wait_for_heartbeat()
        logging.info('node %s initialized', node_id)
        reply(msg, type='init_ok')

    elif msg.body.type == 'read':
        if state == State.LEADER:
            if msg.body.key in linkv:
                logging.info('read %s', msg.body.key)
                reply(msg, type='read_ok', value=linkv[msg.body.key])

            else:
                send_error(msg, '20', 'Key not found')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            send_error(msg, '11', 'Not leader')

    elif msg.body.type == 'write':
        logging.info('write %s', msg.body.key)
        if state == State.LEADER:
            index = len(log) + 1
            c = Command('write', msg.body.key, msg.body.value, currentTerm, index, msg.src)
            log.append(c)       
            matchIndex[node_id] = index
        else:
            send_error(msg, '11', 'Not leader')
        
    elif msg.body.type == 'cas':
        logging.info('cas %s', msg.body.key)
        if state == State.LEADER:
            if msg.body.key in linkv:
                if getattr(msg.body, "from") == linkv[msg.body.key]:
                    index = len(log) + 1
                    c = Command('write', msg.body.key, msg.body.to, currentTerm, index, msg.src)
                    log.append(c)
                    matchIndex[node_id] = index
                else:
                    send_error(msg, '22', 'From value does not match key')
                    logging.warning('from does not match value for key %s', msg.body.key)
            else:
                send_error(msg, '21', 'Key not found')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            send_error(msg, '11', 'Not leader')

    elif msg.body.type == 'log_replication':
        logging.info('log replication')

        if currentTerm > msg.body.term:
            reply(msg, type='log_replication_resp', success=False, term=currentTerm)
        elif verify_heartbeat(msg):
            reply(msg, type='log_replication_resp', success=True, term=currentTerm)
            wait_for_heartbeat()
        else:
            reply(msg, type='log_replication_resp', success=False, term=msg.body.term)
            wait_for_heartbeat()

    elif msg.body.type == 'log_replication_resp':
        logging.info('log replication response')
        if msg.body.success == True:
            requests = log[nextIndex[msg.src]:]
            nextIndex[msg.src] += len(requests)
            matchIndex[msg.src] = nextIndex[msg.src]
            for request in requests:
                majority = 0
                for node in node_ids:
                    logging.info('matchIndex %s %s', node, matchIndex[node])
                    if matchIndex[node] >= request.index:
                        majority += 1
                #request.resp += 1
                #If there a majority of replicas responses, apply the command to the state machine
                #if request.resp > (len(node_ids)//2):
                if majority == (len(node_ids)//2) + 1:
                    logging.info('commit %s %s', request.key, request.value)
                    linkv[request.key] = request.value
                    lastApplied += 1
                    commitIndex += 1
                    send(node_id, request.src, type='write_ok')
                else:
                    break 
        elif msg.body.term > currentTerm:
            currentTerm = msg.body.term
            state = State.FOLLOWER
            votedFor = None
            wait_for_heartbeat()    
        else:
            nextIndex[msg.src] -= 1

    elif msg.body.type == 'request_vote':
        logging.info('request vote')
            
        #Reply false if term < currentTerm
        if (msg.body.term > currentTerm):
            currentTerm = msg.body.term
            if state != State.LEADER:
                heartbeat_timeout.cancel()
                if state == State.CANDIDATE:
                    logging.info('cancel election timeout')
                    election_timeout.cancel()
            state = State.FOLLOWER
            votedFor = None
            if (msg.body.lastLogIndex > len(log)-1):
                votedFor = msg.body.candidate_id
                reply(msg, type='request_vote_resp', vote_granted=True, term=currentTerm)
            else:
                reply(msg, type='request_vote_resp', vote_granted=False, term=currentTerm)
        elif (msg.body.term == currentTerm) and (votedFor == None or votedFor == msg.body.candidate_id) and (msg.body.lastLogIndex > len(log)-1):
                votedFor = msg.body.candidate_id
                reply(msg, type='request_vote_resp', vote_granted=True, term=currentTerm)
        else:
            reply(msg, type='request_vote_resp', vote_granted=False, term=currentTerm)
        if state != State.LEADER:
            wait_for_heartbeat()


    elif msg.body.type == 'request_vote_resp':
        logging.info('request vote response')
        if currentTerm == msg.body.term and msg.body.vote_granted == True and state == State.CANDIDATE:
            vote_count += 1
            if vote_count > (len(node_ids)//2):
                heartbeat_timeout.cancel()
                election_timeout.cancel()
                state = State.LEADER
                votedFor = None
                for i in node_ids:
                    nextIndex[i] = len(log)
                    matchIndex[i] = 0
                logging.info('leader elected')
                Thread(target=send_heartbeat).start()
        elif msg.body.term > currentTerm:
            currentTerm = msg.body.term
            state = State.FOLLOWER
            election_timeout.cancel()
            votedFor = None
            wait_for_heartbeat()
