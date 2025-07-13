use std::{
    collections::HashMap,
    fs::{self, File},
    io::{Error, ErrorKind, Read},
};

use crate::database::{
    column::Column,
    schema::Schema,
    table::Table,
    utils::{DataType, InputDataEnum},
};

pub struct DatabaseInterface {
    pub is_connect: bool,
    pub database: Option<Schema>,
}

impl DatabaseInterface {
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
                true
            }
            Err(err) => match err.kind() {
                ErrorKind::NotFound => false,
                _ => panic!("Something went wrong"),
            },
        }
    }

    pub fn create_database(&self, database_name: &String) -> bool {
        let new_schema = Schema {
            name: database_name.clone(),
            tables: Vec::new(),
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
                true
            }
            Err(_) => false,
        }
    }

    // TODO
    pub fn show_database() {}

    pub fn list_all_table(&self) -> Vec<String> {
        let check_database = match &self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.get_table_list_array_string()
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
            let name = match column.get("name") {
                Some(val) => val.to_owned(),
                None => panic!("Please input key name!"),
            };
            let data_type = match column.get("type") {
                Some(val) => {
                    if val == "string" {
                        DataType::String
                    } else if val == "integer" {
                        DataType::Integer
                    } else {
                        panic!("Incorrect Type")
                    }
                }
                None => panic!("Please input key data type!"),
            };
            let col = Column {
                name: name,
                data_type: data_type,
                rows: Vec::new(),
            };
            table.add_column(col);
        }
        match &mut self.database {
            Some(database) => {
                database.add_table(table);
                true
            }
            None => {
                panic!("Please Connect to database")
            }
        }
    }

    pub fn drop_table(&mut self, table_name: &String) {
        let check_database = match &mut self.database {
            Some(database) => database,
            None => {
                panic!("Please select database first!")
            }
        };

        check_database.delete_table(table_name.to_owned())
    }
}
