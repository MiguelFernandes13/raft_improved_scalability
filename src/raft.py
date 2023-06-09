#!/usr/bin/env python
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
currentLeader = None
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
    def __init__(self, type, key, value, term, index, src, client = None):
        self.type = type
        self.key = key
        self.value = value
        self.term = term
        self.index = index
        self.src = src
        self.client = client

def broadcast(**kwds):
    for i in node_ids:
        if i != node_id:
            send(node_id, i, **kwds)

def request_vote():
    global state, currentTerm, vote_count, votedFor, election_timeout, log
    if state != State.LEADER:
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
                if nextIndex[i] < len(log):
                    send(node_id, i, type='log_replication', term=currentTerm,
                            leader_id=node_id, prevLogIndex=prevLogIndex,
                            prevLogTerm = prevLogTerm, entries=log[nextIndex[i]:],
                            leadercommit=commitIndex)
                else:
                    send(node_id, i, type='log_replication', term=currentTerm, leader_id=node_id,
                            prevLogIndex=prevLogIndex, prevLogTerm = prevLogTerm, entries=[], leadercommit=commitIndex)
        time.sleep(0.07)

# Reply false if term < currentTerm
def verify_term(msg):
    global currentTerm, votedFor, state, currentLeader
    if msg.body.term >= currentTerm:
        currentLeader = msg.body.leader_id
        if msg.body.term > currentTerm:
            become_follower(msg)
        else:
            heartbeat_timeout.cancel()
        return True
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
        wait_for_heartbeat()   
        if verify_prevLogIndex(msg):
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
                commitIndex = min(msg.body.leadercommit, len(log))
                for i in range(lastApplied, commitIndex ):
                    linkv[log[i].key] = log[i].value
                lastApplied = commitIndex
            return True
    return False


def send_error(msg, code, text, client=None):
    msg.body.code = code
    msg.body.text = text
    if client:
        reply(msg, type='error', client=client)
    else:
        reply(msg, type='error')

def create_command(msg):
    global currentTerm, log, matchIndex
    index = len(log) + 1
    if msg.body.type == 'cas':
        if hasattr(msg.body, 'client'):
            c = Command('cas', msg.body.key, msg.body.to, currentTerm, index, msg, msg.body.client)
        else:
            c = Command('cas', msg.body.key, msg.body.to, currentTerm, index, msg)
    else:
        if hasattr(msg.body, 'client'):
            c = Command('write', msg.body.key, msg.body.value, currentTerm, index, msg, msg.body.client)
        else:
            c = Command('write', msg.body.key, msg.body.value, currentTerm, index, msg)
    log.append(c)       
    matchIndex[node_id] = index

def become_follower(msg):
    global state, votedFor, currentTerm, heartbeat_timeout, election_timeout
    if state != State.LEADER:
        heartbeat_timeout.cancel()
        if state == State.CANDIDATE:
            election_timeout.cancel()
    state = State.FOLLOWER
    votedFor = None
    currentTerm = msg.body.term
    
    
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
        logging.info('read %s', msg.body.key)
        if state == State.LEADER:
            if msg.body.key in linkv:
                #check if msg.body has attribute client
                if hasattr(msg.body, "client"): 
                    reply(msg, type='read_ok', value=linkv[msg.body.key], client=msg.body.client)
                else:
                    reply(msg, type='read_ok', value=linkv[msg.body.key])

            else:
                if hasattr(msg.body, "client"): 
                    send_error(msg, '20', 'Key not found', client=msg.body.client)
                else:
                    send_error(msg, '20', 'Key not found')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            if currentLeader:
                send(node_id, currentLeader, type='read', key=msg.body.key, client=msg)
            else:
                send_error(msg, '11', 'Not leader')


    elif msg.body.type == 'write':
        logging.info('write %s', msg.body.key)
        if state == State.LEADER:
            create_command(msg)
        else:
            if currentLeader:
                send(node_id, currentLeader, type='write', key=msg.body.key, value=msg.body.value, client=msg)
            else:
                send_error(msg, '11', 'Not leader')
        
    elif msg.body.type == 'cas':
        logging.info('cas %s', msg.body.key)
        if state == State.LEADER:
            if msg.body.key in linkv:
                if (hasattr(msg.body, "from") and getattr(msg.body, "from") == linkv[msg.body.key]) or (hasattr(msg.body, "ffrom") and getattr(msg.body, "ffrom") == linkv[msg.body.key]):
                    create_command(msg)
                else:
                    if hasattr(msg.body, "client"):
                        send_error(msg, '22', 'From value does not match key', client=msg.body.client)
                    else:
                        send_error(msg, '22', 'From value does not match key')
                    logging.warning('from does not match value %s for key %s', msg.body.key, linkv[msg.body.key])
            else:
                if hasattr(msg.body, "client"):
                    send_error(msg, '21', 'Key not found', client=msg.body.client)
                else:
                    send_error(msg, '21', 'Key not found')
                logging.warning('value not found for key %s', msg.body.key)
        else:
            if currentLeader and hasattr(msg.body, "from"):
                send(node_id, currentLeader, type='cas', key=msg.body.key, ffrom = getattr(msg.body, "from"), to=msg.body.to, client=msg)
            else:
                send_error(msg, '11', 'Not leader')


    elif msg.body.type == 'log_replication':
        logging.info('log replication')
        if verify_heartbeat(msg):
            reply(msg, type='log_replication_resp', success=True, term=currentTerm, matchIndex=len(log))
        else:
            reply(msg, type='log_replication_resp', success=False, term=msg.body.term)

    elif msg.body.type == 'log_replication_resp':
        logging.info('log replication response')
        if msg.body.success == True:
            requests = log[nextIndex[msg.src]:msg.body.matchIndex]
            nextIndex[msg.src] += len(requests)
            matchIndex[msg.src] = nextIndex[msg.src]
            for request in requests:
                majority = 0
                for node in node_ids:
                    if matchIndex[node] >= request.index:
                        majority += 1
                #If there a majority of replicas responses, apply the command to the state machine
                if majority == (len(node_ids)//2) + 1:
                    linkv[request.key] = request.value
                    lastApplied += 1
                    commitIndex += 1
                    if request.type == 'write':
                        if request.client != None:
                            reply(request.src, type='write_ok', client=request.client)
                        else:
                            reply(request.src, type='write_ok')
                    elif request.type == 'cas':
                        if request.client != None:
                            reply(request.src, type='cas_ok', client=request.client)
                        else:
                            reply(request.src, type='cas_ok')
                else:
                    break 
        elif msg.body.term > currentTerm:
            become_follower(msg)
            wait_for_heartbeat()    
        else:
            nextIndex[msg.src] -= 1

    elif msg.body.type == 'request_vote':
        logging.info('request vote')        
        #Reply false if term < currentTerm
        if (msg.body.term > currentTerm):
            become_follower(msg)
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
                Thread(target=send_heartbeat).start()
        elif msg.body.term > currentTerm:
            become_follower(msg)
            wait_for_heartbeat()

    elif msg.body.type == 'write_ok':
        reply(msg.body.client, type='write_ok')

    elif msg.body.type == 'cas_ok':
        reply(msg.body.client, type='cas_ok')

    elif msg.body.type == 'read_ok':
        reply(msg.body.client, type='read_ok', value=msg.body.value)

    elif msg.body.type == 'error':
        reply(msg.body.client, type='error')
