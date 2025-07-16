use std::{
    collections::{HashMap, HashSet},
    fs::File,
    future::pending,
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
use zbus::{interface, Connection, Result};

mod database;

#[test]
fn test() {
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

    // TestDatabaseInterface::test_update_data("articles".to_string(), "users".to_string());
    // TestDatabaseInterface::test_create_join_data();
    TestDatabaseInterface::test_get_join_data("articles".to_string());
    // TestDatabaseInterface::print("articles".to_string());
}

struct DatabaseConnection {
    db_interface: DatabaseInterface,
}

#[interface(name = "org.two.DatabaseConnection")]
impl DatabaseConnection {
    async fn say_hello(&self, name: &str) -> String {
        format!("Test: {}", name)
    }

    async fn select_database(&mut self, database_name: &str) -> String {
        let res = self
            .db_interface
            .select_database(&database_name.to_string());
        match res {
            true => "Connected to database".to_string(),
            false => "Database not found".to_string(),
        }
    }

    async fn create_database(&mut self, database_name: &str) -> String {
        let res = self
            .db_interface
            .create_database(&database_name.to_string());
        match res {
            true => "Database Created!".to_string(),
            false => "Database Failed to create".to_string(),
        }
    }

    async fn drop_database(&mut self, database_name: &str) -> String {
        let res = self
            .db_interface
            .create_database(&database_name.to_string());
        match res {
            true => "Database Dropped!".to_string(),
            false => "Failed to Drop Database".to_string(),
        }
    }

    async fn list_table(&mut self) -> Vec<String> {
        let res = self.db_interface.list_all_table();
        res
    }

    async fn create_table(&mut self, table_name: &str) -> String {
        let column = vec![];
        let res = self
            .db_interface
            .create_table(&table_name.to_string(), column);
        match res {
            true => "Table Created".to_string(),
            false => "Failed to create table".to_string(),
        }
    }

    async fn drop_table(&mut self, table_name: &str) -> String {
        let res = self.db_interface.drop_table(&table_name.to_string());
        match res {
            true => "Table Dropped".to_string(),
            false => "Failed to drop table".to_string(),
        }
    }

    async fn add_column(&mut self, table_name: &str, name: &str, data_type: &str) -> String {
        let res = self.db_interface.add_column_to_table(
            &table_name.to_string(),
            name.to_string(),
            data_type.to_string(),
        );
        match res {
            true => "Column Created".to_string(),
            false => "Failed to create column".to_string(),
        }
    }

    async fn list_column(&mut self, table_name: &str) -> Vec<String> {
        let res = self
            .db_interface
            .list_column_on_table(table_name.to_string());
        res
    }

    async fn delete_column(&mut self, table_name: &str, column_name: &str) -> String {
        let res = self
            .db_interface
            .delete_column_on_table(table_name.to_string(), column_name.to_string());
        match res {
            true => "Column Deleted".to_string(),
            false => "Failed to delete column".to_string(),
        }
    }

    async fn add_data(&mut self, table_name: &str, data: HashMap<String, String>) -> String {
        let res = self
            .db_interface
            .add_data(&table_name.to_string(), data);
        match res {
            true => "Data Created".to_string(),
            false => "Failed to create data".to_string(),
        }
    }

    //  TODO
    async fn get_data(&mut self, table_name: &str) -> Vec<HashMap<String, String>> {
        let res = self
            .db_interface
            .get_data(&table_name.to_string());
        res 
    }
    // TODO
    async fn get_data(&mut self, table_name: &str) -> Vec<HashMap<String, String>> {
        let res = self
            .db_interface
            .get_data(&table_name.to_string());
        res 
    }

    // #[zbus(property)]
    // async fn greeter_name(&self) -> &str{
    //     // &self.name
    //     "str"
    // }
    // #[zbus(property)]
    // async fn set_greeter_name(&mut self, name: &str){
    //     // self.name = name;
    // }
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_connection = DatabaseConnection {
        db_interface: DatabaseInterface {
            is_connect: false,
            database: None,
        },
    };

    let connection: Connection = Connection::session().await?;

    connection
        .object_server()
        .at("/org/two/DatabaseConnection", database_connection)
        .await?;

    connection
        .request_name("org.two.DatabaseConnection")
        .await?;

    loop {
        pending::<()>().await;
    }
}
