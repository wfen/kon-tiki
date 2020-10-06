#!/usr/bin/env python

from __future__ import unicode_literals
import sys
import json
from pprint import pformat


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
        sys.stderr.write("Received\n" + pformat(msg) + "\n")


Net().process_msg()
