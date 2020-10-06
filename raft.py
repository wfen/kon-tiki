#!/usr/bin/env python

from __future__ import unicode_literals
import sys
import json
from pprint import pformat
import datetime
import select
import time
import traceback

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


class Net:
    """Handles console IO for sending and receiving messages."""

    def __init__(self):
        """Constructs a new network client."""
        self.node_id = None
        self.handlers = {}  # A map of message types to handler functions
        self.callbacks = {}  # A map of message IDs to response handlers

    def set_node_id(self, id):
        self.node_id = id

    def on(self, msg_type, handler):
        """Register a callback for a message of the given type."""
        if msg_type in self.handlers:
            raise RuntimeError("already have a handler for message type " + type)

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

    def process_msg(self):
        """Handles a message from stdin, if one is currently available."""
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


class RaftNode:
    def __init__(self):
        self.node_id = None  # Our node ID
        self.node_ids = None  # The set of node IDs

        self.state = "nascent"  # One of nascent, follower, candidate, or leader

        self.net = Net()

    def set_node_id(self, id):
        """Assign our node ID."""
        self.node_id = id
        self.net.set_node_id(id)

    def main(self):
        """Entry point"""
        log("Online.")

        # Handle initialization message
        def raft_init(msg):
            body = msg["body"]
            self.set_node_id(body["node_id"])
            self.node_ids = body["node_ids"]
            log("I am:", self.node_id)
            self.net.reply(msg, {"type": "raft_init_ok"})

        self.net.on("raft_init", raft_init)

        while True:
            try:
                self.net.process_msg() or time.sleep(0.001)
            except KeyboardInterrupt:
                log("Aborted by interrupt!")
                break
            except:
                log("Error!", traceback.format_exc())


RaftNode().main()
