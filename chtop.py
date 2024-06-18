#!/usr/bin/env python3
""" Simple top-like utility for ClickHouse. """

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

import copy
import curses
import json
import time

import requests

# Settings
CH_HOST = "127.0.0.1"
CH_PORT = "8123"
CH_URL = f"http://{CH_HOST}:{CH_PORT}"
CH_QUERY_TIMEOUT = 10

PROCESSES_TABLE = "system.processes"
REFRESH = 2  # seconds
MAPPINGS = dict(
    [
        ("ID", "query_id"),
        ("User", "user"),
        ("PUser", ("_extra", "CHProxy-User")),
        ("Initial?", "is_initial_query"),
        ("Host", "address"),
        ("RAddress", ("_extra", "RemoteAddr")),
        ("Time", "elapsed"),
        ("Query", "query"),
    ]
)

DEFAULT_STATUS = (
    "[q]: quit; [e]: export report; [p]: (un)pause updates; [?]: display this message"
)

EXPORT_FILE_PATTERN = "/tmp/chtop_export_{}.json"


class CHTopSession:
    """Create and query ClickHouse session."""

    def __init__(self):
        self.last_query_result = None
        self.processes = []

        self.is_closed = False
        self.is_paused = False

    def _do_query(self, query_text, json_result=True):
        with requests.request(
            "get", CH_URL, params={"query": query_text}, timeout=CH_QUERY_TIMEOUT
        ) as resp:
            if json_result:
                val = resp.json()
            else:
                val = resp.text()

        return val

    def close(self):
        """End session."""

        self.is_closed = True

    def pause(self):
        """Toggle pause."""

        self.is_paused = not self.is_paused

    def fetch_processes(self):
        """Fetch the current state of ClickHouse process table."""

        self.last_query_result = self._do_query(
            f"SELECT * FROM {PROCESSES_TABLE} FORMAT JSON"
        )
        self.processes = copy.deepcopy(self.last_query_result["data"])

        for p in self.processes:
            ua_string = p["http_user_agent"]

            ua_extra_values = [v.strip() for v in ua_string.split(";")]
            for v in ua_extra_values:
                if ":" not in v:
                    continue
                p1, p2 = v.split(":", 1)
                p1 = p1.strip()
                p2 = p2.strip()
                p[("_extra", p1)] = p2
            p["query"] = " ".join(p["query"].splitlines())

    def export(self):
        """Export latest query information as json."""

        filename = EXPORT_FILE_PATTERN.format(time.time())
        with open(filename, "w", encoding="UTF-8") as f:
            json.dump(self.last_query_result, f, indent=8)

        return filename


class CHTopUI:
    """Manages top-like curses UI."""

    def __init__(self, session):
        self.session = session

        screen = curses.initscr()

        curses.curs_set(0)
        curses.noecho()
        curses.cbreak()
        curses.halfdelay(int(REFRESH * 10 + 0.5))
        screen.keypad(0)

        self.screen = screen
        self.rows, self.cols = screen.getmaxyx()

        self.format_string = (
            "{:<36}  {:<13}  {:<10}  {:<8}  {:<25} {:<16}  {:<15}  {:<60}"
        )

        self.status = DEFAULT_STATUS

    def cleanup(self):
        """Restore terminal."""

        self.screen.keypad(0)
        self.screen.nodelay(False)

        curses.curs_set(1)
        curses.echo()
        curses.nocbreak()
        curses.endwin()

    def resize(self):
        """Updates UI state on terminal resize."""

        self.rows, self.cols = self.screen.getmaxyx()

    def format_entries(self, data):
        """Formats data provided using pre-defined format string."""

        return self.format_string.format(*data)[: self.cols - 1]

    def draw(self):
        """Draw UI."""

        self.screen.clear()

        header = self.format_entries(MAPPINGS.keys())
        self.screen.addstr(0, 0, header, curses.A_REVERSE | curses.A_BOLD)

        last_table_row = self.rows - 2
        status_row = self.rows - 1

        self.screen.addstr(status_row, 0, self.status)

        for line_no, process_data in enumerate(self.session.processes):
            line_pos = line_no + 1
            if line_pos >= last_table_row:
                self.screen.addstr(line_pos, 0, "....")
                break

            process_line = self.format_entries(
                [process_data.get(v, "N/A") for v in MAPPINGS.values()]
            )
            self.screen.addstr(line_pos, 0, process_line)

        self.screen.refresh()

    def handle_user_input(self):
        """Handle user input"""

        key = self.screen.getch()

        if key == curses.KEY_RESIZE:
            self.resize()
        elif key == ord("q"):
            self.session.close()
        elif key == ord("e"):
            export_file = self.session.export()
            self.status = f"Exported to: {export_file}"
        elif key == ord("p"):
            self.session.pause()
            if self.session.is_paused:
                self.status = "Updates paused"
            else:
                self.status = "Updates resumed"
        elif key != -1:
            self.status = DEFAULT_STATUS


class CHTop:
    """Manages CHTop state, initialization and graceful shutdown."""

    def __init__(self):
        self.session = CHTopSession()
        self.ui = CHTopUI(self.session)

    def cleanup(self):
        """Gracefully exit."""

        self.ui.cleanup()

    def updater(self):
        """Task running updates every {REFRESH} seconds."""

        while not self.session.is_closed:
            if not self.session.is_paused:
                self.session.fetch_processes()
            self.ui.draw()
            self.ui.handle_user_input()

    def main(self):
        """Program entry point."""

        try:
            self.updater()
        except KeyboardInterrupt:
            self.session.close()
        finally:
            self.cleanup()


app = CHTop()
app.main()
