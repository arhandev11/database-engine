from dbus_next.aio import MessageBus

import asyncio

loop = asyncio.get_event_loop()

async def main():
    bus = await MessageBus().connect()
    introspection = await bus.introspect('org.two.DatabaseConnection', '/org/two/DatabaseConnection')
    obj = bus.get_proxy_object('org.two.DatabaseConnection', '/org/two/DatabaseConnection', introspection)
    connection = obj.get_interface('org.two.DatabaseConnection')
    
    res = await connection.call_create_database("tests") # Membuat database test
    res = await connection.call_select_database("tests") # Select database test
    res = await connection.call_drop_database("tests") # Mencoba menghapus database test

    res = await connection.call_create_database("library") # Membuat database
    res = await connection.call_select_database("library") # Select database
    res = await connection.call_create_table("books") # Membuat table
    res = await connection.call_list_table() # Melihat list table
    res = await connection.call_add_column("books", "id", "integer") # Membuat Kolom baru
    res = await connection.call_add_column("books", "name", "string") # Membuat Kolom baru
    res = await connection.call_add_column("books", "author", "string") # Membuat Kolom baru
    res = await connection.call_delete_column("books", "author") # Membuat kolom author sebagai bahan uji penghapusan kolom
    res = await connection.call_list_column("books") # Melihat list kolom pada tabel book
    res = await connection.call_add_data("books", {'id': "0", "name": "Farhan"}) # Membuat data baru pada tabel books

    print(res)

loop.run_until_complete(main())