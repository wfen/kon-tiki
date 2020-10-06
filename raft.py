#!/usr/bin/env python

from __future__ import unicode_literals
import sys
import json
from pprint import pformat
import datetime
import time

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
        pass

    def process_msg(self):
        """Handles a message from stdin, if one is currently available."""
        line = sys.stdin.readline()
        if not line:
            return None

        msg = json.loads(line)
        return msg


class RaftNode:
    def __init__(self):
        self.node_id = None  # Our node ID
        self.node_ids = None  # The set of node IDs

        self.state = "nascent"  # One of nascent, follower, candidate, or leader

        self.net = Net()

    def main(self):
        """Entry point"""
        log("Online.")

        # Handle initialization message
        msg = self.net.process_msg()
        body = msg["body"]
        if body["type"] != "raft_init":
            raise RuntimeError("Unexpected message " + pformat(msg))

        self.node_id = body["node_id"]
        self.node_ids = body["node_ids"]
        log("I am:", self.node_id)


RaftNode().main()
