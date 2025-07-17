use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
};

use crate::database::{
    cell::Cell,
    column::Column,
    table::{self, Table},
    utils::{self, bytes_to_string, DataType, InputDataEnum},
};

pub struct Schema {
    pub name: String,
    pub tables: Vec<Table>,
    pub index: HashMap<String, Vec<HashMap<String, InputDataEnum>>>,
}

impl Schema {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        let mut name_str_len = utils::integer_to_bytes(self.name.len() as isize).to_vec();
        bytes.append(&mut name_str_len);
        let mut name_bytes = utils::string_to_bytes(self.name.clone());
        bytes.append(&mut name_bytes);

        let mut table_len = utils::integer_to_bytes(self.tables.len() as isize).to_vec();
        bytes.append(&mut table_len);

        for table in self.tables.iter() {
            bytes.append(&mut table.to_bytes());
        }
        bytes
    }

    pub fn to_data(buf_u8: &mut Vec<u8>) -> Schema {
        // Parsing bytes to table name length first
        let rest_of_bytes = buf_u8.to_vec();
        let (name_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let name_len_u8: [u8; 8] = name_len.try_into().unwrap();
        let name_len_usize = usize::from_le_bytes(name_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Parsing bytes to the value of table name
        let rest_of_bytes = buf_u8.to_vec();
        let (name_u8, rest_of_bytes) = rest_of_bytes.split_at(name_len_usize);
        let name = bytes_to_string(name_u8.to_vec());
        *buf_u8 = rest_of_bytes.to_vec();

        // Parsing bytes to table length
        let rest_of_bytes = buf_u8.to_vec();
        let (table_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let table_len_u8: [u8; 8] = table_len.try_into().unwrap();
        let table_len_usize = usize::from_le_bytes(table_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Start Parsing the column
        let mut tables: Vec<Table> = Vec::new();

        for i in 0..table_len_usize {
            let new_col = Table::to_data(buf_u8);
            tables.push(new_col);
        }

        let mut schema = Schema {
            name,
            tables,
            index: HashMap::new(),
        };

        schema.build_index();

        schema
    }

    pub fn save(&mut self) -> std::io::Result<()> {
        let path: String = "schema/".to_owned() + &self.name;
        let mut file = File::create(path)?;
        let buf_result = self.to_bytes();
        file.write_all(&buf_result)?;
        self.clear_index();
        Ok(())
    }

    pub fn create_table(&mut self, table: Table) {
        if self.check_table_index(table.name.clone()) != -1 {
            panic!("Table Already Exists");
        }
        self.tables.push(table);
        let _ = self.save();
    }

    pub fn add_column_to_table(&mut self, table_name: String, mut column: Column) -> bool {
        let table = self.search_table(table_name);
        match column.data_type {
            DataType::String => {
                column.rows = (1..=table.length)
                    .map(|x| Cell {
                        data_type: DataType::String,
                        data_value: utils::string_to_bytes("".to_owned()),
                    })
                    .collect();
            }
            DataType::Integer => {
                column.rows = (1..=table.length)
                    .map(|x| Cell {
                        data_type: DataType::String,
                        data_value: utils::integer_to_bytes(0).to_vec(),
                    })
                    .collect();
            }
            DataType::Null => {}
        }
        table.add_column(column);
        let _ = self.save();
        true
    }

    pub fn list_column_on_table(&mut self, name: String) -> Vec<String> {
        let table = self.search_table(name);
        table.get_column_names()
    }

    pub fn delete_column_on_table(&mut self, table_name: String, column_name: String) -> bool {
        let table = self.search_table(table_name);

        table.delete_column(column_name);
        let _ = self.save();

        true
    }

    pub fn search_table(&mut self, name: String) -> &mut Table {
        let mut selected_table: Option<&mut Table> = None;
        for table in &mut self.tables {
            if table.name == name {
                selected_table = Some(table);
            }
        }

        match selected_table {
            Some(table) => table,
            None => {
                panic!("Table not Found")
            }
        }
    }

    pub fn check_table_index(&self, name: String) -> isize {
        let mut index = -1;

        let mut loop_index = 0;
        for table in &self.tables {
            if table.name == name {
                index = loop_index;
                break;
            }
            loop_index += 1;
        }
        index
    }

    pub fn drop_table(&mut self, table_name: String) {
        let table_index = self.check_table_index(table_name);

        if table_index == -1 {
            panic!("Table not Found");
        }

        self.tables.remove(table_index as usize);
        let _ = self.save();
    }

    pub fn add_data(&mut self, table_name: String, data: HashMap<String, String>) -> bool {
        let table = self.search_table(table_name);
        table.add_data(data);
        let _ = self.save();
        true
    }

    pub fn get_data(&mut self, table_name: String) -> Vec<HashMap<String, InputDataEnum>> {
        let table = self.search_table(table_name);
        let result = table.get_data();
        result
    }

    pub fn search_data(
        &mut self,
        table_name: String,
        column_name: String,
        value: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let table = self.search_table(table_name);
        let result = table.search_by_column(column_name, value);
        result
    }

    pub fn update_data(
        &mut self,
        table_name: String,
        where_data: HashMap<String, String>,
        updated_data: HashMap<String, String>,
    ) -> bool {
        let table = self.search_table(table_name);
        let result = table.update_data(where_data, updated_data);
        let _ = self.save();
        result
    }

    pub fn delete_data(&mut self, table_name: String, where_data: HashMap<String, String>) -> bool {
        let table = self.search_table(table_name);
        let result = table.delete_data(where_data);
        let _ = self.save();
        result
    }

    pub fn join_table(
        &mut self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
        join_type: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let result: Vec<HashMap<String, InputDataEnum>> = match join_type.as_str() {
            "inner" => self.join_inner_table(table_name, column_name, table_join, column_join),
            "left" => self.left_right_join_table(table_name, column_name, table_join, column_join),
            "right" => self.left_right_join_table(table_join, column_join, table_name, column_name),
            _ => {
                panic!("Invalid Join Type")
            }
        };
        result
    }

    pub fn left_right_join_table(
        &mut self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let selected_table = self.search_table(table_name.to_string());
        let mut left_join: Vec<HashMap<String, InputDataEnum>> = selected_table.get_data();
        let selected_join_table = self.search_table(table_join.to_string());

        let mut result: Vec<HashMap<String, InputDataEnum>> = Vec::new();

        for item in &mut left_join {
            let val = match item.get(&column_name) {
                Some(input) => input,
                None => panic!("Key tidak ditemukan"),
            };

            let join_result = match val {
                InputDataEnum::String(word) => {
                    selected_join_table.search_by_column(column_join.clone(), word.clone())
                }
                InputDataEnum::Integer(num) => {
                    selected_join_table.search_by_column(column_join.clone(), num.to_string())
                }
                InputDataEnum::Null => Vec::new(),
            };

            let mut res: HashMap<String, InputDataEnum> = HashMap::new();
            if join_result.len() > 0 {
                match join_result.first() {
                    Some(item) => {
                        for (k, v) in item {
                            let value = match v {
                                InputDataEnum::Integer(num) => InputDataEnum::Integer(*num),
                                InputDataEnum::String(word) => {
                                    InputDataEnum::String(word.to_string())
                                }
                                InputDataEnum::Null => InputDataEnum::Null,
                            };
                            res.insert(k.to_string(), value);
                        }
                    }
                    None => {}
                }
            }
            for (k, v) in item {
                let value = match v {
                    InputDataEnum::Integer(num) => InputDataEnum::Integer(*num),
                    InputDataEnum::String(word) => InputDataEnum::String(word.to_string()),
                    InputDataEnum::Null => InputDataEnum::Null,
                };
                res.insert(k.to_string(), value);
            }

            result.push(res);
        }

        result
    }

    pub fn join_inner_table(
        &mut self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let selected_table = self.search_table(table_name.to_string());
        let left_join: Vec<HashMap<String, InputDataEnum>> = selected_table.get_data();
        let selected_join_table = self.search_table(table_join.to_string());

        let mut result: Vec<HashMap<String, InputDataEnum>> = Vec::new();

        for item in &left_join {
            let val = match item.get(&column_name) {
                Some(input) => input,
                None => panic!("Key tidak ditemukan"),
            };

            let join_result = match val {
                InputDataEnum::String(word) => {
                    selected_join_table.search_by_column(column_join.clone(), word.clone())
                }
                InputDataEnum::Integer(num) => {
                    selected_join_table.search_by_column(column_join.clone(), num.to_string())
                }
                InputDataEnum::Null => Vec::new(),
            };

            for res_search in join_result {
                let mut res: HashMap<String, InputDataEnum> = HashMap::new();
                for (k, v) in res_search {
                    res.insert(k, v);
                }
                for (k, v) in item {
                    let value = match v {
                        InputDataEnum::Integer(num) => InputDataEnum::Integer(*num),
                        InputDataEnum::String(word) => InputDataEnum::String(word.to_string()),
                        InputDataEnum::Null => InputDataEnum::Null,
                    };
                    res.insert(k.to_string(), value);
                }
                result.push(res);
            }
        }

        result
    }

    pub fn build_index(&mut self) {
        let index_file_name: String = "index/".to_owned() + &self.name;
        let mut file = Schema::get_file(index_file_name);
        let mut buf = Vec::new();
        file.read_to_end(&mut buf);

        // If file empty then assign an empty map
        let decoded_index: HashMap<String, Vec<HashMap<String, InputDataEnum>>>;
        if buf.len() == 0 {
            decoded_index = HashMap::new();
        } else {
            decoded_index = bincode::deserialize(&buf[..]).unwrap();
        }
        self.index = decoded_index;
    }

    pub fn save_index(
        &mut self,
        key: String,
        result: Vec<HashMap<String, InputDataEnum>>,
    ) -> std::io::Result<()> {
        let index_file_name: String = "index/".to_owned() + &self.name;
        let mut file = File::create(index_file_name)?;
        // If file empty then assign an empty map
        self.index.insert(key.clone(), result);
        let index_u8: Vec<u8> = bincode::serialize(&self.index).unwrap();
        file.write_all(&index_u8)?;
        Ok(())
    }

    pub fn clear_index(&mut self) -> std::io::Result<()> {
        let index_file_name: String = "index/".to_owned() + &self.name;
        let mut file = File::create(index_file_name)?;
        // If file empty then assign an empty map
        let decoded_index: HashMap<String, Vec<HashMap<String, InputDataEnum>>> = HashMap::new();
        let index_u8: Vec<u8> = bincode::serialize(&decoded_index).unwrap();
        file.write_all(&index_u8)?;
        self.index = decoded_index;
        Ok(())
    }

    pub fn print(&self) {
        println!("Database {:?}:", self.name);
        for table in self.tables.iter() {
            table.print();
        }
        println!();
        println!();
    }

    pub fn list_all_table(&self) -> Vec<String> {
        let mut table_names: Vec<String> = Vec::new();
        for table in self.tables.iter() {
            table_names.push(table.name.clone());
        }

        table_names
    }

    fn get_file(name: String) -> File {
        let read_file_res = File::open(name.as_str());
        let file = match read_file_res {
            Ok(read_file) => read_file,
            Err(err) => {
                let created_file_res = File::create(name.as_str());
                match created_file_res {
                    Ok(created_file) => created_file,
                    Err(err) => panic!("Failed Open file: {:?}", err),
                };

                // After Create File we need to open it again so its not failing
                let read_file_res = File::open(name.as_str());
                let file = match read_file_res {
                    Ok(read_file) => read_file,
                    Err(err) => {
                        panic!("Failed Open file: {:?}", err)
                    }
                };
                file
            }
        };
        file
    }
}
