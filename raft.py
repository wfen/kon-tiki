#!/usr/bin/env python

from __future__ import unicode_literals
import sys
import json
from pprint import pformat
import datetime
import select
import time
import traceback
import random

# Utilities


def log(*args):
    """Helper function for logging stuff to stderr"""
    first = True
    sys.stderr.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f "))
    for i in range(len(args)):
        sys.stderr.write(str(args[i]))
        if i < (len(args) - 1):
            sys.stderr.write(" ")
    sys.stderr.write("\n")
    sys.stderr.flush()


class Net:
    """Handles console IO for sending and receiving messages."""

    def __init__(self):
        """Constructs a new network client."""
        self.node_id = None  # Our local node ID
        self.next_msg_id = 0  # The next message ID we're going to allocate
        self.handlers = {}  # A map of message types to handler functions
        self.callbacks = {}  # A map of message IDs to response handlers

    def set_node_id(self, id):
        self.node_id = id

    def new_msg_id(self):
        """Generate a fresh message ID"""
        id = self.next_msg_id
        self.next_msg_id += 1
        return id

    def on(self, msg_type, handler):
        """Register a callback for a message of the given type."""
        if msg_type in self.handlers:
            raise RuntimeError("already have a handler for message type " + msg_type)

        self.handlers[msg_type] = handler

    def send_msg(self, msg):
        """Sends a raw message object"""
        log("Sent\n" + pformat(msg))
        json.dump(msg, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def send(self, dest, body):
        """Sends a message to the given destination node with the given body."""
        self.send_msg({"src": self.node_id, "dest": dest, "body": body})

    def reply(self, req, body):
        """Replies to a given request message with a response body."""
        body["in_reply_to"] = req["body"]["msg_id"]
        self.send(req["src"], body)

    def rpc(self, dest, body, handler):
        """Sends an RPC request to dest and handles the response with handler."""
        msg_id = self.new_msg_id()
        self.callbacks[msg_id] = handler
        body["msg_id"] = msg_id
        self.send(dest, body)

    def process_msg(self):
        """Handles a message from stdin, if one is currently available."""

        # If we don't have any stdin, we'll return None, and if we do get
        # a message and process it, we'll return True.
        if sys.stdin not in select.select([sys.stdin], [], [], 0)[0]:
            return None

        line = sys.stdin.readline()
        if not line:
            return None

        msg = json.loads(line)
        log("Received\n" + pformat(msg))
        body = msg["body"]

        handler = None
        # Look up reply handler
        if "in_reply_to" in body:
            m = body["in_reply_to"]
            handler = self.callbacks[m]
            del self.callbacks[m]

        # Fall back based on message type
        elif body["type"] in self.handlers:
            handler = self.handlers[body["type"]]

        else:
            raise RuntimeError("No callback or handler for\n" + pformat(msg))

        handler(msg)
        return True


class Log:
    """Stores Raft entries, which are dicts with a :term field."""

    def __init__(self):
        """Construct a new Log"""
        # Note that we provide a default entry here, which simplifies
        # some default cases involving empty logs.
        self.entries = [{"term": 0, "op": None}]

    def get(self, i):
        """Return a log entry by index. Note that Raft's log is 1-indexed."""
        return self.entries[i - 1]

    def append(self, entries):
        """Appends multiple entries to the log."""
        self.entries.extend(entries)
        log("Log:\n" + pformat(self.entries))

    def last(self):
        """Returns the most recent entry"""
        return self.entries[-1]

    def size(self):
        "How many entries are in the log?"
        return len(self.entries)


class KVStore:
    def __init__(self):
        self.state = {}

    def apply(self, op):
        """Applies an op to the state machine, and returns a response message"""
        t = op["type"]
        k = op["key"]

        # Handle state transition
        if t == "read":
            if k in self.state:
                res = {"type": "read_ok", "value": self.state[k]}
            else:
                res = {"type": "error", "code": 20, "text": "not found"}
        elif t == "write":
            self.state[k] = op["value"]
            res = {"type": "write_ok"}
        elif t == "cas":
            if k not in self.state:
                res = {"type": "error", "code": 20, "text": "not found"}
            elif self.state[k] != op["from"]:
                res = {
                    "type": "error",
                    "code": 22,
                    "text": "expected "
                    + str(op["from"])
                    + " but had "
                    + str(self.state[k]),
                }
            else:
                self.state[k] = op["to"]
                res = {"type": "cas_ok"}

        log("KV:\n" + pformat(self.state))

        # Construct response
        res["in_reply_to"] = op["msg_id"]
        return {"dest": op["client"], "body": res}


class RaftNode:
    def __init__(self):
        # Heartbeats & timeouts
        self.election_timeout = 2  # Time before election, in seconds
        self.election_deadline = 0  # Next election, in epoch seconds

        # Node & cluster IDS
        self.node_id = None  # Our node ID
        self.node_ids = None  # The set of node IDs

        # Raft state
        self.state = "nascent"  # One of nascent, follower, candidate, or leader
        self.current_term = 0  # Our current Raft term

        # Components
        self.net = Net()
        self.log = Log()
        self.state_machine = KVStore()
        self.setup_handlers()

    def other_nodes(self):
        """All nodes except this one."""
        nodes = list(self.node_ids)
        nodes.remove(self.node_id)
        return nodes

    def set_node_id(self, id):
        """Assign our node ID."""
        self.node_id = id
        self.net.set_node_id(id)

    def brpc(self, body, handler):
        """Broadcast an RPC message to all other nodes, and call handler with each response."""
        for node in self.other_nodes():
            self.net.rpc(node, body, handler)

    def maybe_step_down(self, remote_term):
        """If remote_term is bigger than ours, advance our term and become a follower."""
        if self.current_term < remote_term:
            log(
                "Stepping down: remote term",
                remote_term,
                "higher than our term",
                self.current_term,
            )
            self.advance_term(remote_term)
            self.become_follower()

    def request_votes(self):
        """Request that other nodes vote for us as a leader"""

        # We vote for ourself
        votes = set([self.node_id])
        term = self.current_term

        def handler(res):
            body = res["body"]
            self.maybe_step_down(body["term"])

            if (
                self.state == "candidate"
                and self.current_term == term
                and body["term"] == self.current_term
                and body["vote_granted"]
            ):
                # We have a vote for our candidacy
                votes.add(res["src"])
                log("Have votes:", pformat(votes))

        # Broadcast vote request
        self.brpc(
            {
                "type": "request_vote",
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.log.size(),
                "last_log_term": self.log.last()["term"],
            },
            handler,
        )

    def reset_election_deadline(self):
        """Don't start an election for a little while."""
        self.election_deadline = time.time() + (
            self.election_timeout * (random.random() + 1)
        )

    def advance_term(self, term):
        """Advance our term to `term`, resetting who we voted for."""
        if not self.current_term < term:
            raise RuntimeError("Can't go backwards")

        self.current_term = term

    # Role transitions

    def become_follower(self):
        """Become a follower"""
        self.state = "follower"
        self.reset_election_deadline()
        log("Became follower for term", self.current_term)

    def become_candidate(self):
        """Become a candidate"""
        self.state = "candidate"
        self.advance_term(self.current_term + 1)
        self.reset_election_deadline()
        log("Became candidate for term", self.current_term)
        self.request_votes()

    # Actions for followers/candidates

    def election(self):
        """If it's been long enough, trigger a leader election."""
        if self.election_deadline < time.time():
            if self.state == "follower" or self.state == "candidate":
                # Let's go!
                self.become_candidate()
            else:
                # We're a leader, or initializing; sleep again
                self.reset_election_deadline()
            return True

    # Message handlers

    def setup_handlers(self):
        """Registers message handlers with this node's client"""

        # Handle initialization message
        def raft_init(msg):
            if self.state != "nascent":
                raise RuntimeError("Can't init twice!")

            body = msg["body"]
            self.set_node_id(body["node_id"])
            self.node_ids = body["node_ids"]

            self.become_follower()

            log("I am:", self.node_id)
            self.net.reply(msg, {"type": "raft_init_ok"})

        self.net.on("raft_init", raft_init)

        # Handle client KV requests
        def kv_req(msg):
            op = msg["body"]
            op["client"] = msg["src"]
            res = self.state_machine.apply(op)
            self.net.send(res["dest"], res["body"])

        self.net.on("read", kv_req)
        self.net.on("write", kv_req)
        self.net.on("cas", kv_req)

    def main(self):
        """Entry point"""
        log("Online.")

        while True:
            try:
                self.net.process_msg() or self.election() or time.sleep(0.001)
            except KeyboardInterrupt:
                log("Aborted by interrupt!")
                break
            except:
                log("Error!", traceback.format_exc())


RaftNode().main()
