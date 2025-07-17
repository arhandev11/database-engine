use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Error, ErrorKind, Read},
};

use crate::database::{
    column::Column,
    schema::Schema,
    table::Table,
    utils::{parse_new_column, DataType, InputDataEnum},
};

pub struct DatabaseInterface {
    pub is_connect: bool,
    pub database: Option<Schema>,
}

impl DatabaseInterface {
    pub fn show_databases(&self) -> Vec<String> {
        let files = fs::read_dir("./schema");

        let result: Vec<String> = match files {
            Ok(dir) => dir
                .map(|dir_entry| match dir_entry {
                    Ok(file) => {
                        let res: String = match file.file_name().to_str() {
                            Some(name) => name.to_string(),
                            None => {
                                panic!("Something went Wrong")
                            }
                        };
                        res
                    }
                    Err(_) => {
                        panic!("Something went Wrong")
                    }
                })
                .collect(),
            Err(_) => {
                panic!("Something went Wrong!")
            }
        };
        result
    }
    pub fn select_database(&mut self, database_name: &String) -> bool {
        let path = "schema/".to_owned() + database_name;
        let open_file = File::open(path);

        match open_file {
            Ok(mut file) => {
                let mut buf = Vec::new();
                let read_res = file.read_to_end(&mut buf);
                match read_res {
                    Ok(_) => (),
                    Err(err) => {
                        panic!("Error: {:?}", err)
                    }
                };
                let parsed_schema = Schema::to_data(&mut buf);
                self.database = Some(parsed_schema);
                self.is_connect = true;
                self.build_index();
                true
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => false,
                _ => panic!("Something went wrong"),
            },
        }
    }

    fn build_index(&mut self) {
        match &mut self.database {
            Some(schema) => {
                let build_index = schema.build_index();
            }
            None => {}
        }
    }

    pub fn create_database(&self, database_name: &String) -> bool {
        let mut new_schema = Schema {
            name: database_name.clone(),
            tables: Vec::new(),
            index: HashMap::new(),
        };
        let _ = new_schema.save();
        true
    }

    pub fn drop_database(&mut self, database_name: &String) -> bool {
        let path = "schema/".to_owned() + database_name;
        let result = fs::remove_file(path);
        match result {
            Ok(_) => {
                match &mut self.database {
                    Some(database) => {
                        if database.name == *database_name {
                            self.database = None;
                            self.is_connect = false;
                        }
                    }
                    None => {}
                };
                let path_index = "index/".to_owned() + database_name;
                let result = fs::remove_file(path_index);
                match result {
                    Ok(_) => true,
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    }

    pub fn list_all_table(&self) -> Vec<String> {
        let check_database = match &self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.list_all_table()
    }

    pub fn create_table(
        &mut self,
        table_name: &String,
        columns: Vec<HashMap<String, String>>,
    ) -> bool {
        let mut table = Table {
            name: table_name.clone(),
            columns: Vec::new(),
            length: 0,
        };
        for column in columns {
            let col = parse_new_column(column);
            table.add_column(col);
        }
        match &mut self.database {
            Some(database) => {
                database.create_table(table);
                true
            }
            None => {
                panic!("Please Connect to database")
            }
        }
    }

    pub fn drop_table(&mut self, table_name: &String) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.drop_table(table_name.to_owned());

        true
    }

    pub fn add_column_to_table(
        &mut self,
        table_name: &String,
        name: String,
        data_type: String,
    ) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        let mut map_column: HashMap<String, String> = HashMap::new();
        map_column.insert("name".to_string(), name);
        map_column.insert("type".to_string(), data_type);

        let column = parse_new_column(map_column);

        check_database.add_column_to_table(table_name.to_owned(), column);
        true
    }

    pub fn list_column_on_table(&mut self, table_name: String) -> Vec<String> {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        let column_names = check_database.list_column_on_table(table_name);
        column_names
    }

    pub fn delete_column_on_table(&mut self, table_name: String, column_name: String) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.delete_column_on_table(table_name, column_name);
        true
    }

    pub fn add_data(&mut self, table_name: &String, data: HashMap<String, String>) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.add_data(table_name.to_owned(), data);

        true
    }

    pub fn get_data(&mut self, table_name: &String) -> Vec<HashMap<String, InputDataEnum>> {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };
        let result = check_database.get_data(table_name.to_owned());
        println!("{:?}", result);
        result
    }

    pub fn search_data(
        &mut self,
        table_name: &String,
        column_name: String,
        value: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };
        let result = check_database.search_data(table_name.to_owned(), column_name, value);

        println!("{:?}", result);
        result
    }

    pub fn update_data(
        &mut self,
        table_name: &String,
        where_data: HashMap<String, String>,
        updated_data: HashMap<String, String>,
    ) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        let result = check_database.update_data(table_name.to_owned(), where_data, updated_data);

        println!("{:?}", result);

        true
    }

    pub fn delete_data(
        &mut self,
        table_name: &String,
        where_data: HashMap<String, String>,
    ) -> bool {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };
        let result = check_database.delete_data(table_name.to_owned(), where_data);

        println!("{:?}", result);

        true
    }

    pub fn join_table(
        &mut self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
        join_type: String,
    ) {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        let key = format!(
            "{}_{}_{}_{}_{}",
            table_name, column_name, table_join, column_join, join_type
        );

        match check_database.index.get(&key) {
            Some(_) => {
                // panic!("Index Saved")
            }
            None => {
                let result = check_database.join_table(
                    table_name,
                    column_name,
                    table_join,
                    column_join,
                    join_type,
                );
                let _ = check_database.save_index(key.clone(), result);
            }
        };

        let result = match check_database.index.get(&key) {
            Some(val) => val,
            None => &vec![],
        };

        println!("{:?}", result);

    }

    pub fn print(&mut self) {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };
        check_database.print();
    }
}
