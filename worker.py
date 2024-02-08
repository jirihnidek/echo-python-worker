import dbus
import dbus.service
import threading

"""
Module implementing worker class
"""


class YggWorker(dbus.service.Object):
    """
    Base class for all yggdrasil worker
    """

    def __init__(self, worker_name: str) -> None:
        """
        Constructor for YggWorker class
        :param worker_name: name of the worker
        """
        self.session_bus = dbus.SessionBus()
        bus_name = dbus.service.BusName(f"com.redhat.Yggdrasil1.Worker1.{worker_name}", self.session_bus)
        object_path = f"/com/redhat/Yggdrasil1/Worker1/{worker_name}"
        dbus.service.Object.__init__(self, dbus.SessionBus(), object_path, bus_name)
        # Required properties
        self.my_props = {
            "RemoteContent": False,
            "Features": {
                "DispatchedAt": "",
                "Version": "1"
            }
        }

    def _transmit(self, addr: str, msg_id: str, response_to: str, metadata: dict, data):
        """
        Thread target for transmitting data to yggdrasil over D-Bus, but it does not work.
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        print(f"Transmitting: {addr}, {msg_id}, {response_to}, {metadata}, {data} ...")
        obj = self.session_bus.get_object(
            "com.redhat.Yggdrasil1.Dispatcher1",
            "/com/redhat/Yggdrasil1/Dispatcher1"
        )
        interface = dbus.Interface(obj, "com.redhat.Yggdrasil1.Dispatcher1")
        result = None
        try:
            result = interface.Transmit(addr, msg_id, response_to, metadata, data)
        except dbus.exceptions.DBusException as err:
            print(f"DBusException: {err}")
        else:
            print(f"Transmitted: {result}")
        return result

    def Transmit(self, addr: str, msg_id: str, response_to: str, metadata: dict, data):
        """
        Try to transmit data using thread
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return:
        """
        print("Starting transmit thread")
        thread = threading.Thread(target=self._transmit, args=(addr, msg_id, response_to, metadata, data))
        thread.start()
        print("Thread started")

    @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature='ss', out_signature='v')
    def Get(self, interface, prop):
        """

        :param interface:
        :param prop:
        :return:
        """
        return self.my_props.get(prop)

    @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature='ssv')
    def Set(self, interface, prop, value):
        """
        Set method for D-Bus property
        :param interface:
        :param prop:
        :param value:
        :return:
        """
        self.my_props[prop] = value

    @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature='s', out_signature='a{sv}')
    def GetAll(self, interface) -> dict:
        """
        Get all D-Bus properties
        :param interface:
        :return:
        """
        return dbus.Dictionary(self.my_props, signature="sv")

    @dbus.service.signal(dbus.PROPERTIES_IFACE, signature='sa{sv}as')
    def PropertiesChanged(self, interface_name, changed_properties, invalidated_properties) -> None:
        """
        Signal triggered, when any property has changed
        :param interface_name:
        :param changed_properties:
        :param invalidated_properties:
        :return:
        """
        print(f"PropertiesChanged: {interface_name}, {changed_properties}, {invalidated_properties}")

    def event_handler(self, signal_name: int, message_id: str, response_to: str, data) -> None:
        raise NotImplementedError

    @dbus.service.signal(dbus_interface='com.redhat.Yggdrasil1.Worker1',
                         signature='ussa{ss}')
    def Event(self, signal_name: int, message_id: str, response_to: str, data) -> None:
        """
        Handler of Event signal
        :param signal_name:
        :param message_id:
        :param response_to:
        :param data:
        :return:
        """
        self.event_handler(signal_name, message_id, response_to, data)

    def dispatch_handler(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        raise NotImplementedError

    @dbus.service.method(dbus_interface='com.redhat.Yggdrasil1.Worker1',
                         in_signature='sssa{ss}ay',
                         out_signature='')
    def Dispatch(self, addr: str, msg_id: str, response_to: str, metadata: dict, data) -> None:
        """
        This method is called when a message is received by yggdrasil server
        and is dispatched to this worker
        :param addr:
        :param msg_id:
        :param response_to:
        :param metadata:
        :param data:
        :return: None
        """
        self.dispatch_handler(addr, msg_id, response_to, metadata, data)

    def cancel_handler(self, directive: str, msg_id: str, cancel_id: str) -> None:
        raise NotImplementedError

    @dbus.service.method(dbus_interface='com.redhat.Yggdrasil1.Worker1',
                         in_signature='sss',
                         out_signature='')
    def Cancel(self, directive: str, msg_id: str, cancel_id: str) -> None:
        """
        This method is called when a cancel command is received by yggdrasil server
        for message  with given cancel_id
        :param directive:
        :param msg_id:
        :param cancel_id:
        :return:
        """
        self.cancel_handler(directive, msg_id, cancel_id)

