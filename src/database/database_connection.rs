use std::collections::HashMap;

use zbus::interface;

use crate::database::database_interface::DatabaseInterface;

pub struct DatabaseConnection {
    pub db_interface: DatabaseInterface,
}

#[interface(name = "org.two.DatabaseConnection")]
impl DatabaseConnection {
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
            .drop_database(&database_name.to_string());
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
        let res = self.db_interface.add_data(&table_name.to_string(), data);
        match res {
            true => "Data Created".to_string(),
            false => "Failed to create data".to_string(),
        }
    }


    async fn update_data(
        &mut self,
        table_name: &str,
        where_data: HashMap<String, String>,
        updated_data: HashMap<String, String>,
    ) -> String {
        let res = self
            .db_interface
            .update_data(&table_name.to_owned(), where_data, updated_data);
        match res {
            true => "Data Updated".to_string(),
            false => "Failed to update data".to_string(),
        }
    }

    async fn delete_data(
        &mut self,
        table_name: &str,
        where_data: HashMap<String, String>,
    ) -> String {
        let res = self
            .db_interface
            .delete_data(&table_name.to_owned(), where_data);
        match res {
            true => "Data Deleted".to_string(),
            false => "Failed to delete data".to_string(),
        }
    }

     //  TODO
    //  async fn get_data(&mut self, table_name: &str) -> Vec<HashMap<String, String>> {
    //     let res = self.db_interface.get_data(&table_name.to_string());
    //     res
    // }
    // // TODO
    // async fn search_data(
    //     &mut self,
    //     table_name: &str,
    //     column_name: String,
    //     value: String,
    // ) -> Vec<HashMap<String, String>> {
    //     let res = self.db_interface.get_data(&table_name.to_string());
    //     res
    // }
    // //  TODO
    // async fn join_table(
    //     &mut self,
    //     table_name: &str,
    //     column_name: &str,
    //     table_join: &str,
    //     column_join: &str,
    //     join_type: &str,
    // ) -> Vec<HashMap<String, String>> {
    //     let res = self.db_interface.join_table(
    //         table_name.to_string(),
    //         column_name.to_string(),
    //         table_join.to_string(),
    //         column_join.to_string(),
    //         join_type.to_string(),
    //     );
    //     res
    // }

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
