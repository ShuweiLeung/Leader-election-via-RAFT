import rpyc
from  rpyc.utils.server import ThreadedServer
import sys
from threading import Timer
from threading import Thread
import os
import random
import time
import logging
from threading import Lock

lock = Lock()

class RaftNode(rpyc.Service):
    """
        Initialize the class using the config file provided and also initialize
        any datastructures.
    """

    def __init__(self, config, ith):
        logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', filename='log' + str(ith) + '.txt',
                            filemode='w',
                            level=logging.DEBUG)
        self.log = []

        self.myIP = None
        self.myPort = None
        self.myId = None

        # state of node
        self.is_leader = False
        self.is_candidate = False
        self.is_follower = True

        # used for election
        self.votedFor = None  # whether this node has voted
        self.votes = 0  # just be used when the state is cacndidate, the votes current node has
        self.currentTerm = 0
        self.latestTerm = 0  # just be used during the elect period to get the current latest term in the network

        logging.info('Node Start0')

        fp = open(config, "r")
        try:
            self.countOfAllNodes = int(fp.readline().split()[1])
            self.NodesList = []
            for i in range(self.countOfAllNodes):
                nodeIP, nodePort = fp.readline().split()[1].split(":")
                nodePort = int(nodePort)
                if i == ith:
                    self.myIP = nodeIP
                    self.myPort = nodePort
                    self.myId = self.myIP + " " + str(self.myPort)
                    continue
                self.NodesList.append((nodeIP, nodePort))
        except Exception:
            logging.info("------------IO Error-----------")
        finally:
            fp.close()

        # one timer can just be started only once except that it is reinitialized
        self.timer_follower = Timer(1 + random.random(), self.convert_to_candidate)
        self.timer_leader = Timer(0.4, self.heartbeat)
        self.timer_select_tie = Timer(1 + random.random(), self.convert_to_candidate)


        self.convert_to_follower()

    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader
        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call
        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''

    def exposed_is_leader(self):
        return self.is_leader

    def exposed_AppendEntries(self, term, leaderId):
        logging.info('someone sends heartbeat')

        if term < self.currentTerm:
            return str((False, self.currentTerm))

        logging.info('The caller is leader')

        # Only when appendEntries is effective, timer_follower is reset
        votedFor_temp = None
        if self.currentTerm == term:
            lock.acquire()
            votedFor_temp = self.votedFor
            lock.release()

        self.convert_to_follower()

        lock.acquire()
        self.votedFor = votedFor_temp
        lock.release()

        self.currentTerm = term

        # storage persistent
        f = open("/tmp/" + self.myId + ".txt", 'w')
        try:
            f.write(str(self.currentTerm) + "-" + str(self.votedFor))
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            logging.info("------------IO Error-----------")
        finally:
            f.close()

        return str((True, self.currentTerm))

    def exposed_RequestVote(self, term, candidateId):
        if term < self.currentTerm:
            return str((False, self.currentTerm))

        if term == self.currentTerm and self.is_candidate:
            return str((False, self.currentTerm))

        if term == self.currentTerm and self.is_leader:
            return str((False, self.currentTerm))

        votedFor_temp = None
        if self.is_follower == True and self.currentTerm == term:
            lock.acquire()
            votedFor_temp = self.votedFor
            lock.release()

        if votedFor_temp == None:
            self.convert_to_follower()

        lock.acquire()
        if self.votedFor != None:
            return str((False, self.currentTerm))
        lock.release()

        # When getting votes successfully, reset the timer_follower
        logging.info("I'm a follower, and I vote for %s", candidateId)

        lock.acquire()
        self.votedFor = candidateId
        self.currentTerm = term
        lock.release()

        # storage persistent
        f = open("/tmp/" + self.myId + ".txt", 'w')
        try:
            f.write(str(self.currentTerm) + "-" + str(self.votedFor))
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            logging.info("------------IO Error-----------")
        finally:
            f.close()

        return str((True, self.currentTerm))

    def convert_to_follower(self):
        logging.info('Node becomes a follower')
        self.is_leader = False
        self.is_candidate = False
        self.is_follower = True

        if os.path.exists(self.myId + ".txt"):
            f = open("/tmp/" + self.myId + ".txt", 'r')
            try:
                line = f.read()
                self.currentTerm = int(line.split("-")[0])
                votedFor_temp = line.split("-")[1]
            except Exception:
                logging.info("------------IO Error-----------")
            finally:
                f.close()

            if votedFor_temp == "None":
                lock.acquire()
                self.votedFor = None
                lock.release()
            else:
                lock.acquire()
                self.votedFor = votedFor_temp
                lock.release()
        else:
            lock.acquire()
            self.votedFor = None  # whether this node has voted
            self.votes = 0  # just be used when the state is cacndidate, the votes current node has
            lock.release()

        self.timer_leader.cancel()
        self.timer_select_tie.cancel()
        self.timer_follower.cancel()
        self.timer_follower = Timer(1 + random.random(), self.convert_to_candidate)
        self.timer_follower.start()

    def convert_to_candidate(self):
        logging.info('Node becomes a candidate | term : %s', str(self.currentTerm + 1))
        self.is_leader = False
        self.is_candidate = True
        self.is_follower = False
        self.currentTerm += 1  # update the value of current term

        # From follower to candidate, timer_follower and timer_leader have been canceled in follower state
        self.timer_follower.cancel()
        self.timer_leader.cancel()
        self.timer_select_tie.cancel()
        self.timer_select_tie = Timer(1 + random.random(), self.convert_to_candidate)
        self.timer_select_tie.start()
        # state changes to candidate

        lock.acquire()
        self.latestTerm = self.currentTerm
        self.votes = 1  # vote for itself
        self.votedFor = self.myId
        lock.release()

        for nodeIP, nodePort in self.NodesList:
            t = Thread(target=self.thread_RequestVote, args=(nodeIP, nodePort))
            t.start()

        logging.info('Node becomes a candidate over')

        # storage persistent
        f = open("/tmp/" + self.myId + ".txt", 'w')
        try:
            f.write(str(self.currentTerm) + "-" + str(self.votedFor))
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            logging.info("------------IO Error-----------")
        finally:
            f.close()

    def convert_to_leader(self):
        logging.info('Node becomes a leader, currentTerm : %s', str(self.currentTerm))

        self.is_leader = True
        self.is_candidate = False
        self.is_follower = False
        self.latestTerm = self.currentTerm

        self.timer_follower.cancel()
        self.timer_select_tie.cancel()
        self.timer_leader.cancel()
        self.timer_leader = Timer(0.4, self.heartbeat)
        self.timer_leader.start()

        # storage persistent
        f = open("/tmp/" + self.myId + ".txt", 'w')
        try:
            f.write(str(self.currentTerm) + "-" + str(self.votedFor))
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            logging.info("------------IO Error-----------")
        finally:
            f.close()

    def heartbeat(self):
        if not self.is_leader:
            return

        logging.info('heartbeating...')
        for nodeIP, nodePort in self.NodesList:
            # conncurent append
            t = Thread(target=self.thread_AppendEntries, args=(nodeIP, nodePort))
            t.start()

        self.timer_leader = Timer(0.4, self.heartbeat)
        self.timer_leader.start()

    def thread_RequestVote(self, nodeIP, nodePort):
        logging.info('[thread]requesting votes...')
        # if the below condition is satisfied, it indicates that former thread of this node has gotten enough votes to become a leader
        # Or another node has become a leader
        if not self.is_candidate:
            return

        nodeConn = rpyc.connect(nodeIP, nodePort)
        response = nodeConn.root.RequestVote(self.currentTerm, self.myId)
        response = eval(response)
        voteOrNot = response[0]
        otherNodeTerm = response[1]
        if voteOrNot:
            self.votes += 1
            logging.info("get one vote, current votes : %s", self.votes)
            nodeConn.close()
            if self.votes > self.countOfAllNodes // 2 and self.currentTerm >= self.latestTerm:
                self.convert_to_leader()
            return

        self.latestTerm = max(self.latestTerm, otherNodeTerm)
        nodeConn.close()

        if otherNodeTerm > self.currentTerm:
            self.currentTerm = otherNodeTerm
            lock.acquire()
            self.votedFor = None
            lock.release()

            # storage persistent
            f = open("/tmp/" + self.myId + ".txt", 'w')
            try:
                f.write(str(self.currentTerm) + "-" + str(self.votedFor))
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                logging.info("------------IO Error-----------")
            finally:
                f.close()

            self.convert_to_follower()

    def thread_AppendEntries(self, nodeIP, nodePort):
        logging.info('[thread]sending heartbeat port: %s', str(nodePort))
        if not self.is_leader:
            return
        nodeConn = rpyc.connect(nodeIP, nodePort)
        response = nodeConn.root.AppendEntries(self.currentTerm, self.myId)
        nodeConn.close()

        successOrNot = response[0]
        otherNodeTerm = response[1]
        if not successOrNot and otherNodeTerm > self.currentTerm:
            self.currentTerm = otherNodeTerm

            lock.acquire()
            self.votedFor = None
            lock.release()

            # storage persistent
            f = open("/tmp/" + self.myId + ".txt", 'w')
            try:
                f.write(str(self.currentTerm) + "-" + str(self.votedFor))
                f.flush()
                os.fsync(f.fileno())
            except Exception:
                logging.info("------------IO Error-----------")
            finally:
                f.close()

            self.convert_to_follower()


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer

    config = sys.argv[1]
    fp = open(config, "r")
    try:
        num = fp.readline().split()[1]
        num = int(num)
    except Exception:
        logging.info("------------IO Error-----------")
    finally:
        fp.close()

    ith = int(sys.argv[2])

    nodePort = int(sys.argv[3])
    server = ThreadedServer(RaftNode(sys.argv[1], ith), port=nodePort)

    server.start()
