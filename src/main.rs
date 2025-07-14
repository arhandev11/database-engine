use std::{
    collections::{HashMap, HashSet},
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
    // TestDatabaseInterface::test_show_database();
    // TestDatabaseInterface::test_create_database("Test 2".to_string());
    // TestDatabaseInterface::test_select_database("Test 2".to_string());
    // TestDatabaseInterface::test_drop_database("Test 3".to_string());
    // TestDatabaseInterface::test_create_table_with_column("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_drop_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_list_table("Test 2".to_string());
    // TestDatabaseInterface::test_add_column(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "text_aja".to_string(),
    //     "string".to_string(),
    // );
    // TestDatabaseInterface::test_list_column_on_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_delete_column_on_table(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "text_aja".to_string(),
    // );
    // TestDatabaseInterface::test_list_column_on_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_add_data("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_get_data("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_search_data(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "first_name".to_string(),
    //     "Akbar".to_string(),
    // );
    // TestDatabaseInterface::test_delete_data("Test 2".to_string(), "table_1".to_string());

    // TestDatabaseInterface::test_update_data("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_create_join_data();
    TestDatabaseInterface::test_get_join_data("articles".to_string());
    // TestDatabaseInterface::print("articles".to_string());
}
