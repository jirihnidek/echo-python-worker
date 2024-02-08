#!/usr/bin/env python

"""
Example of simple Echo worker using D-Bus API
"""

from gi.repository import GLib
from gi.repository import GObject
from dbus.mainloop.glib import DBusGMainLoop
from worker import YggWorker
import dbus
import argparse
import uuid
import time


class EcoWorker(YggWorker):

    NAME = "echo"

    def __init__(self, loop_count: int = 1, sleep_time: float = 0) -> None:
        super().__init__(self.NAME)
        print(f"Running '{self.NAME}' worker: {self}, loop_count: {loop_count}, sleep_time: {sleep_time}")
        self.loop_count = loop_count
        self.sleep_time = sleep_time

    def send_echo_message(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Send a message to the yggd
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        self.Transmit(addr, msg_id, response_to, metadata, data)

    def event_handler(self, signal_name: int, message_id: str, response_to: str, data) -> None:
        """
        Handler of D-Bus signal
        :param signal_name:
        :param message_id:
        :param response_to:
        :param data:
        :return:
        """
        print(f"Signal: {signal_name} {message_id} {response_to} {data}")

    def dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        Handler of message received over D-Bus from yggdrasil server
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        print(f"Dispatch: {addr} {msg_id} {response_to} {metadata} {data}")
        for i in range(self.loop_count):
            print(f"Sending echo message {i}...")
            self.send_echo_message(
                addr=addr,
                msg_id=str(uuid.uuid4()),
                response_to=msg_id,
                metadata=metadata,
                data=data
            )
            if self.sleep_time > 0:
                print(f"Sleeping for {self.sleep_time} seconds...")
                time.sleep(self.sleep_time)
        print("Dispatch done")

    def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
        """
        Handler of cancel command received over D-Bus from yggdrasil server
        :param directive:
        :param msg_id:
        :param cancel_id:
        :return:
        """
        print(f"Cancel: {directive}, {msg_id}, {cancel_id}")


def _main():
    parser = argparse.ArgumentParser(
        description="Example of eco worker")
    parser.add_argument(
        "--loop",
        type=int,
        default=1,
        help="number of loop echoes before finish echoing. (default 1)")
    parser.add_argument(
        "--sleep",
        default=0,
        type=float,
        help="sleep time in seconds before echoing the response")
    args = parser.parse_args()

    DBusGMainLoop(set_as_default=True)

    EcoWorker(loop_count=args.loop, sleep_time=args.sleep)

    GObject.threads_init()
    dbus.mainloop.glib.threads_init()
    mainloop = GLib.MainLoop()
    mainloop.run()


if __name__ == '__main__':
    _main()
