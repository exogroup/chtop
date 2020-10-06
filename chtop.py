#!/usr/bin/env python

#
# Copyright 2020 EXADS
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

from time import sleep
from collections import OrderedDict
import threading
import os
import Queue
import tty
import re
import requests
import urllib
import sys
import datetime
import termios

# Settings
CH_HOST = '127.0.0.1'
CH_PORT = '8123'
PROCESSES_TABLE = 'system.processes'
REFRESH = 3 # seconds
URL = 'http://' + CH_HOST + ':' + CH_PORT + '?'
MAPPINGS = OrderedDict([
    ('ID', 'query_id'),
    ('User', 'user'),
    ('PUser', 'http_user_agent'),
    ('Host', 'address'),
    ('RAddress', 'http_user_agent'),
    ('Time', 'elapsed'),
    ('Query', 'query')
  ]
)

# Global variables
rows, columns = os.popen('stty size', 'r').read().split() # window size
pressed_key = Queue.Queue()
orig_settings = termios.tcgetattr(sys.stdin) # Keep original stdin settings

# Useful class to control font colors
class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

# Trivial function to clear the screen
def clear(): 
  _ = os.system('clear') 

# Function to query CH
def ch_query(query):
  url = URL + urllib.urlencode( { 'query' : query } )
  try:
    response = requests.get(url)
  except Exception, e:
    sys.exit("Problem with connectiong to ClickHouse: %s. Exiting." % str(e))

  return response

def processes_pretty_output(data):
  clear()

  # Build the final data to print
  data_to_print = []
  for row in data:
    values = []
    for mapping in MAPPINGS:
      column = MAPPINGS[mapping]
      if column == 'elapsed':
        value = str(datetime.timedelta(seconds=row[column]))
      elif column == 'http_user_agent':
        if mapping == 'PUser':
          puser = re.search('CHProxy-User: ([a-zA-Z0-9-]+)', row[column])
          if puser:
            value = puser.group(1)
          else:
            value = 'N/A'
        elif mapping == 'RAddress':
          host = re.search('RemoteAddr: (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):', row[column])
          if host:
            value = host.group(1)
          else:
            value = 'N/A'
      else:
        value = row[column]
      values.append(value.replace("\n", " ").rstrip())
    data_to_print.append(values)

  # Print the data nicely
  print_format = "{:<36}  {:<13}  {:<10}  {:<25} {:<15}  {:<15}  {:<60}"
  print(color.BOLD + print_format.format(*MAPPINGS.keys()) + color.END)
  for row in data_to_print:
    print(print_format.format(*row))[0:int(columns)]

# Function used as a thread that polls for pressed key
def key_poller(key_ready, stop):
  # Don't wait for ENTER after key press
  tty.setcbreak(sys.stdin)

  key = 0
  while key != 'q':
    if not stop.isSet():
      key = sys.stdin.read(1)[0]
      pressed_key.put(key)
      key_ready.set()
      sleep(0.2)
    
  # Reset stdin settings (reverts no ENTER behavior)
  termios.tcsetattr(sys.stdin, termios.TCSADRAIN, orig_settings) 

# Function used as a thread that displays active processes
def show_processes(stop, kill):
  query = 'SELECT * FROM ' + PROCESSES_TABLE + ' FORMAT JSON'

  while True:
    if not stop.isSet():
      response = ch_query(query)
      processes_pretty_output(response.json()['data'])

    if kill.isSet():
      break;

    try:
      if not stop.isSet(): sleep(REFRESH)
    except KeyboardInterrupt:
      break

# Function to handle full query displaying
def show_full_query():
  query = "SELECT query FROM system.query_log WHERE query_id = '{}' AND event_date = today()"

  termios.tcsetattr(sys.stdin, termios.TCSADRAIN, orig_settings)
  query_id = raw_input(color.UNDERLINE + 'Query ID to analyze:' + color.END + ' ')
  tty.setcbreak(sys.stdin)
  response = ch_query(query.format(query_id))
  clear()
  print(response.text)
  sys.stdin.read(1)[0]

# Function to handle query kill command
def kill_query():
  query = "KILL QUERY WHERE query_id = '{}'"

  termios.tcsetattr(sys.stdin, termios.TCSADRAIN, orig_settings)
  query_id = raw_input(color.UNDERLINE + 'Query ID to kill:' + color.END + ' ')
  tty.setcbreak(sys.stdin)
  ch_query(query.format(query_id))
  clear()
  print('KILL query send')
  sys.stdin.read(1)[0]

# Main function
def main():
  # Threads' events
  stop_processes = threading.Event() # controls 'show processes' thread
  stop_key_poller = threading.Event() # controls 'key_poller' thread
  key_ready = threading.Event() # is set once the key was pressed, used in the 'key_poller' thread
  kill_flag = threading.Event() # is set once the program is expected to be finished

  # Threads definition and start
  show_processes_t = threading.Thread(target = show_processes, args = (stop_processes, kill_flag))
  show_processes_t.start()
  key_poller_t = threading.Thread(target = key_poller, args = (key_ready, stop_key_poller))
  key_poller_t.start()

  # Main loop
  key = 0
  while key != 'q':
    stop_processes.clear() # 'activate' 'show processes' thread
    if key_ready.isSet():
      key = pressed_key.get()
      key_ready.clear()
      if key == 'f':
        stop_processes.set()
        stop_key_poller.set()
        show_full_query()
        stop_key_poller.clear()
      elif key == 'k':
        stop_processes.set()
        stop_key_poller.set()
        kill_query()
        stop_key_poller.clear()

  # If we are here, it means we want to exit
  kill_flag.set()

if __name__ == '__main__':
  main()

