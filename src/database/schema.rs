use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Write},
};

use crate::database::{
    table::{self, Table},
    utils::{self, bytes_to_string, InputDataEnum},
};

pub struct Schema {
    pub name: String,
    pub tables: Vec<Table>,
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

        Schema { name, tables }
    }

    pub fn save(&self) -> std::io::Result<()> {
        let path: String = "schema/".to_owned() + &self.name;
        let mut file = File::create(path)?;
        let buf_result = self.to_bytes();
        file.write_all(&buf_result)?;
        Ok(())
    }

    pub fn add_table(&mut self, table: Table) {
        if self.check_table_index(table.name.clone()) != -1 {
            panic!("Table Already Exists");
        }
        self.tables.push(table);
        let _ = self.save();
    }

    pub fn search_table(&self, name: String) -> &Table {
        let mut selected_table: Option<&Table> = None;
        for table in &self.tables {
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

    pub fn delete_table(&mut self, table_name: String) {
        
        let table_index = self.check_table_index(table_name); 

        if table_index == -1 {
            panic!("Table not Found");
        }
        
        self.tables.remove(table_index as usize);
        let _ = self.save();
    }

    pub fn get_data_join(
        &self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
    ) -> std::io::Result<Vec<HashMap<String, InputDataEnum>>> {
        // Reading Index File
        let mut index_file = self.get_file("index".to_string());
        let mut buf = Vec::new();

        let read_res = index_file.read_to_end(&mut buf);
        match read_res {
            Ok(_) => (),
            Err(err) => {
                panic!("Error: {:?}", err)
            }
        };

        // If file empty then assign an empty map
        let decoded: HashMap<String, Vec<u8>>;
        if buf.len() == 0 {
            decoded = HashMap::new();
        } else {
            decoded = bincode::deserialize(&buf[..]).unwrap();
        }

        let result_data_u8 = decoded.get(
            format!(
                "{}_{}_{}_{}",
                table_name, column_name, table_join, column_join
            )
            .as_str(),
        );

        // Checking key exists, if exists then return in, if not then build index
        let result: Vec<HashMap<String, InputDataEnum>> = match result_data_u8 {
            Some(vec_u8) => {
                let decoded_result: Vec<HashMap<String, InputDataEnum>> =
                    bincode::deserialize(&vec_u8[..]).unwrap();
                decoded_result
            }
            None => {
                let result = self.build_index(
                    table_name.to_string(),
                    column_name.to_string(),
                    table_join.to_string(),
                    column_join.to_string(),
                );
                match result {
                    Ok(data) => data,
                    Err(err) => {
                        panic!("Error building index: {}", err)
                    }
                }
            }
        };
        Ok(result)
    }

    pub fn build_index(
        &self,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
    ) -> std::io::Result<Vec<HashMap<String, InputDataEnum>>> {
        // Reading Index File
        let mut file = File::open("index")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        // If file empty then assign an empty map
        let mut decoded_index: HashMap<String, Vec<u8>>;
        if buf.len() == 0 {
            decoded_index = HashMap::new();
        } else {
            decoded_index = bincode::deserialize(&buf[..]).unwrap();
        }

        // Reading building index
        let selected_table = self.search_table(table_name.to_string());
        let mut result: Vec<HashMap<String, InputDataEnum>> = selected_table.get_data();
        let selected_join_table = self.search_table(table_join.to_string());

        for item in &mut result {
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
                InputDataEnum::Null => HashMap::new(),
            };

            item.extend(join_result);
        }

        let result_u8: Vec<u8> = bincode::serialize(&result).unwrap();
        decoded_index.insert(
            format!(
                "{}_{}_{}_{}",
                table_name, column_name, table_join, column_join
            ),
            result_u8,
        );
        let index_u8: Vec<u8> = bincode::serialize(&decoded_index).unwrap();
        let mut file = File::create("index")?;
        file.write_all(&index_u8)?;

        Ok(result)
    }

    pub fn print(&self) {
        println!("Database {:?}:", self.name);
        for table in self.tables.iter() {
            table.print();
        }
        println!();
        println!();
    }

    pub fn get_table_list_array_string(&self) -> Vec<String> {
        let mut table_names: Vec<String> = Vec::new();
        for table in self.tables.iter() {
            table_names.push(table.name.clone());
        }

        table_names
    }

    fn get_file(&self, name: String) -> File {
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
