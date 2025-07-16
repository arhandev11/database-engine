from dbus_next.aio import MessageBus

import asyncio

loop = asyncio.get_event_loop()


async def main():
    bus = await MessageBus().connect()
    # the introspection xml would normally be included in your project, but
    # this is convenient for development
    introspection = await bus.introspect('org.zbus.MyGreeter', '/org/zbus/MyGreeter')

    obj = bus.get_proxy_object('org.zbus.MyGreeter', '/org/zbus/MyGreeter', introspection)
    player = obj.get_interface('org.zbus.MyGreeter')
    res = await player.call_say_hello("Farhan")
    print(res)
    # res = await player.set_greeter_name("FURAIHAN")
    res = await player.get_greeter_name()
    print(res)
    # call methods on the interface (this causes the media player to play)




loop.run_until_complete(main())