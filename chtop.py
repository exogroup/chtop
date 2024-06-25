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
import curses.ascii
import json
import time
from collections import OrderedDict

import requests

# Settings
CH_HOST = "127.0.0.1"
CH_PORT = "8123"
CH_URL = f"http://{CH_HOST}:{CH_PORT}"
CH_QUERY_TIMEOUT = 10

PROCESSES_TABLE = "system.processes"
REFRESH = 2  # seconds
MAPPINGS = OrderedDict(
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
    "[q]: quit; [e]: export report; [p]: (un)pause updates; "
    "[s]: select a query; [?]: display this message"
)

SELECT_MODE_STATUS = (
    "[up/down arrows]: navigate; [r]: manual refresh; "
    "[e]: export selected; [k]: kill; [x]: return"
)

KILL_CONFIRM_MESSAGE = (
    "Are you sure? [Type upper case Y to confirm, any other key to cancel]"
)

EXPORT_FILE_PATTERN = "/tmp/chtop_export_{}.{}"


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
                val = resp.text

        return val

    def close(self):
        """End session."""

        self.is_closed = True

    def pause(self):
        """Toggle pause."""

        self.is_paused = not self.is_paused

    def kill(self, task_id):
        """Try to kill specified task."""
        result = self._do_query(
            f"KILL QUERY WHERE query_id = '{task_id}'", json_result=False
        )
        return result

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

    def export_report(self):
        """Export all information received with latest update as json."""

        filename = EXPORT_FILE_PATTERN.format(time.time(), "json")
        with open(filename, "w", encoding="UTF-8") as f:
            json.dump(self.last_query_result, f, indent=8)

        return filename

    def export_single(self, idx):
        """Export single query string."""

        filename = EXPORT_FILE_PATTERN.format(time.time(), "sql")
        with open(filename, "w", encoding="UTF-8") as f:
            f.write(self.get_details(idx)["query"])

        return filename

    def get_details(self, idx):
        """Get detailed information about a specific entry."""

        return self.last_query_result["data"][idx]


class CHTopUI:
    """Manages top-like curses UI."""

    def __init__(self, session):
        self.session = session

        screen = curses.initscr()

        curses.curs_set(0)
        curses.noecho()
        curses.cbreak()
        curses.halfdelay(int(REFRESH * 10 + 0.5))
        screen.keypad(1)

        self.screen = screen
        self.rows, self.cols = screen.getmaxyx()

        self.format_string = (
            "{:<36}  {:<13}  {:<10}  {:<8}  {:<25} {:<16}  {:<15}  {:<60}"
        )

        self.status = DEFAULT_STATUS

        self.selected_line = 0
        self.select_mode = False

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
        flags = 0
        if not self.select_mode:
            flags = curses.A_REVERSE | curses.A_BOLD
        self.screen.addstr(0, 0, header, flags)

        last_table_row = self.rows - 2
        status_row = self.rows - 1

        offset = 0
        if self.select_mode:
            offset = max(0, self.selected_line - last_table_row + (self.rows // 2))

        self.screen.addstr(status_row, 0, self.status[: self.cols - 1])

        for line_no, process_data in enumerate(self.session.processes):
            line_pos = line_no + 1 - offset
            if line_pos < 1:
                continue

            if line_pos >= last_table_row:
                self.screen.addstr(line_pos, 0, "....")
                break

            process_line = self.format_entries(
                [process_data.get(v, "N/A") for v in MAPPINGS.values()]
            )

            flags = 0

            if self.select_mode and self.selected_line == line_no:
                flags = curses.A_REVERSE | curses.A_BOLD

            self.screen.addstr(line_pos, 0, process_line, flags)

        self.screen.refresh()

    def handle_user_input(self):
        """Handle user input"""

        key = self.screen.getch()

        if key == curses.KEY_RESIZE:
            self.resize()
            self.draw()
        elif key == ord("q"):
            self.session.close()
        elif self.select_mode:
            if key == curses.KEY_UP:
                self.selected_line = max(0, self.selected_line - 1)
            elif key == curses.KEY_DOWN:
                self.selected_line = min(
                    len(self.session.processes) - 1, self.selected_line + 1
                )
            elif key == ord("e"):
                export_file = self.session.export_single(self.selected_line)
                self.status = f"Exported to: {export_file}"
            elif key == ord("x"):
                self.select_mode = False
                self.selected_line = 0
                self.status = DEFAULT_STATUS
            elif key == ord("r"):
                self.session.fetch_processes()
                self.selected_line = 0
                self.draw()
            elif key == ord("k"):
                task_id = self.session.get_details(self.selected_line)["query_id"]
                self.status = KILL_CONFIRM_MESSAGE
                self.draw()

                key = -1
                while key == -1:
                    key = self.screen.getch()

                if key == ord("Y"):
                    self.session.kill(task_id)
                    self.status = f"Attempted to kill task with ID '{task_id}'"
                    self.session.fetch_processes()
                    self.selected_line = 0
                    self.draw()
                else:
                    self.status = "Cancelled."
            elif key != -1:
                self.status = SELECT_MODE_STATUS
        else:
            if key == ord("e"):
                export_file = self.session.export_report()
                self.status = f"Exported to: {export_file}"
            elif key == ord("p"):
                self.session.pause()
                if self.session.is_paused:
                    self.status = "Updates paused"
                else:
                    self.status = "Updates resumed"
            elif key == ord("s"):
                self.select_mode = True
                self.status = SELECT_MODE_STATUS
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
            if not self.session.is_paused and not self.ui.select_mode:
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
