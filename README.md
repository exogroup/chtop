## Overview

chtop is a top-like utility for real-time ClickHouse queries monitoring. It
aims to resemble [innotop](https://github.com/innotop/innotop).

Please note that chtop is in very early stage, so some quirks are expected.
Contributions are more than welcome!

## Usage

Simply execute `chtop.py` script on the ClickHouse server and it will start
displaying currntly running queries with refresh interval of 3 seconds.

The following keys are supported:

* `q` - exit the program
* `f` - asks for the query ID and shows its full version
* `k` - asks for the query ID and kills it asynchronously
