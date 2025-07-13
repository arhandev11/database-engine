use std::{
    collections::HashMap,
    fs::File,
    io::{ErrorKind, Write},
    result,
};

use crate::database::{
    column::Column,
    database_interface::DatabaseInterface,
    schema::Schema,
    table::{self, Table},
    test_interface::TestDatabaseInterface,
    utils::{self, DataType, InputDataEnum},
};

mod database;

fn main() {
    // TestDatabaseInterface::test_create_database("Test 3".to_string());
    // TestDatabaseInterface::test_select_database("Test 3".to_string());
    // TestDatabaseInterface::test_drop_database("Test 3".to_string());
    // TestDatabaseInterface::test_create_table_with_column("Test 2".to_string(), "table_2".to_string());
    TestDatabaseInterface::test_drop_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_list_table("Test 2".to_string());
}
