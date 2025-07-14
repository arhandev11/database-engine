use std::collections::{HashMap, HashSet};

use crate::database::{
    cell::Cell,
    column::Column,
    utils::{self, bytes_to_string, DataType, InputDataEnum},
};

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub length: usize,
}

impl Table {
    fn get_type(&self, name: String) -> DataType {
        for column in &self.columns {
            if column.name == name {
                return column.get_data_type();
            }
        }
        return DataType::Null;
    }

    fn get_data_by_column(&self) -> HashMap<String, Vec<InputDataEnum>> {
        let mut result: HashMap<String, Vec<InputDataEnum>> = HashMap::new();
        for column in &self.columns {
            let mut column_data: Vec<InputDataEnum> = Vec::new();
            for cell in &column.rows {
                column_data.push(cell.value());
            }
            result.insert(column.get_name(), column_data);
        }

        result
    }

    pub fn get_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|col| col.get_name()).collect()
    }

    pub fn get_data(&self) -> Vec<HashMap<String, InputDataEnum>> {
        let mut result: Vec<HashMap<String, InputDataEnum>> = Vec::new();
        for index_row in 0..self.length {
            let mut row_data: HashMap<String, InputDataEnum> = HashMap::new();
            for column in &self.columns {
                let cell: &Cell = &column.rows[index_row];
                row_data.insert(column.get_name(), cell.value());
            }
            result.push(row_data);
        }

        result
    }

    fn get_data_by_index(&self, index: usize) -> HashMap<String, InputDataEnum> {
        let mut row_data: HashMap<String, InputDataEnum> = HashMap::new();
        for column in &self.columns {
            let cell: &Cell = &column.rows[index];
            row_data.insert(column.get_name(), cell.value());
        }
        row_data
    }

    pub fn search_by_column(
        &self,
        column_name: String,
        search: String,
    ) -> Vec<HashMap<String, InputDataEnum>> {
        let mut result: Vec<HashMap<String, InputDataEnum>> = Vec::new();
        let selected_column = self.search_column(column_name);
        for index in 0..self.length {
            match selected_column.rows[index].value() {
                InputDataEnum::String(word) => {
                    if search == word {
                        result.push(self.get_data_by_index(index));
                        true
                    } else {
                        false
                    }
                }
                InputDataEnum::Integer(num) => {
                    if search.as_str().parse::<isize>().unwrap() == num {
                        result.push(self.get_data_by_index(index));
                        true
                    } else {
                        false
                    }
                }
                InputDataEnum::Null => false,
            };
        }

        result
    }

    pub fn search_column(&self, name: String) -> &Column {
        let mut selected_column: Option<&Column> = None;
        for column in &self.columns {
            if column.name == name {
                selected_column = Some(column);
            }
        }

        match selected_column {
            Some(column) => column,
            None => {
                panic!("{:?}, Column not Found", name)
            }
        }
    }

    pub fn print(&self) {
        println!("===Table {:?}===", self.name);
        println!("Column: {:?}", self.get_column_names());
        for index_data in 0..self.length {
            println!("Data {:?}:", index_data + 1);
            for column in self.columns.iter() {
                column.rows[index_data].print();
            }
            println!();
        }
        println!();
        println!();
    }

    pub fn add_column(&mut self, column: Column) {
        if self.check_column_index(column.name.clone()) != -1 {
            panic!("Column Already Exists");
        }
        self.columns.push(column);
    }

    pub fn check_column_index(&self, name: String) -> isize {
        let mut index = -1;

        let mut loop_index = 0;
        for column in &self.columns {
            if column.name == name {
                index = loop_index;
                break;
            }
            loop_index += 1;
        }
        index
    }

    pub fn add_data_column(&mut self, name: String, input: InputDataEnum) {
        for column in &mut self.columns {
            if column.name == name {
                column.insert_data(&input);
                self.length += 1;
                break;
            }
        }
    }

    pub fn add_data(&mut self, input_data: HashMap<String, String>) {
        for item in self.columns.iter_mut() {
            match input_data.get(&item.get_name()) {
                Some(val) => {
                    let value_enum = match item.get_data_type() {
                        DataType::String => InputDataEnum::String(val.to_owned()),
                        DataType::Integer => {
                            InputDataEnum::Integer(val.as_str().parse::<isize>().unwrap())
                        }
                        DataType::Null => {
                            panic!("Not Supported Yet!")
                        }
                    };
                    item.insert_data(&value_enum);
                }
                None => {
                    item.insert_default_data();
                }
            };
        }
        self.length += 1;
    }

    pub fn update(&mut self, index: usize, map: &HashMap<String, String>) {
        for (key, value) in map.iter() {
            for column in &mut self.columns {
                if column.name == *key {
                    let value_enum = match column.get_data_type() {
                        DataType::String => InputDataEnum::String(value.to_owned()),
                        DataType::Integer => {
                            InputDataEnum::Integer(value.as_str().parse::<isize>().unwrap())
                        }
                        DataType::Null => {
                            panic!("Not Supported Yet!")
                        }
                    };
                    column.update_data(index, &value_enum);
                    break;
                }
            }
        }
    }

    pub fn delete_column(&mut self, column_name: String) {
        let column_index = self.check_column_index(column_name);

        if column_index == -1 {
            panic!("Column not Found");
        }

        self.columns.remove(column_index as usize);
    }

    pub fn delete(&mut self, index: usize) {
        for col in &mut self.columns {
            col.delete(index);
        }
        self.length -= 1;
    }

    pub fn update_data(
        &mut self,
        where_data: HashMap<String, String>,
        updated_data: HashMap<String, String>,
    ) -> bool {
        let mut updated_column_index_set: HashSet<usize> = HashSet::new();
        for (column_key, column_val) in where_data.iter() {
            let column = self.search_column(column_key.to_string());
            let updated_column_index = column.search_for_index(column_val.to_string());
            let mut current_updated_column_index_set: HashSet<usize> = HashSet::new();
            updated_column_index.iter().for_each(|i| {
                current_updated_column_index_set.insert(*i);
            });
            if updated_column_index_set.is_empty() {
                updated_column_index_set = current_updated_column_index_set;
            } else {
                // https://users.rust-lang.org/t/intersection-of-hashsets/32351/2
                updated_column_index_set = updated_column_index_set
                    .intersection(&current_updated_column_index_set)
                    .copied()
                    .collect();
            }
            for i in updated_column_index {
                // pengurangan dilakukan agar index yang terpilih sesuai dengan data yang dituju
                // karena setiap delete akan menggeser data yang ada
                self.update(i, &updated_data);
            }
        }
        true
    }

    pub fn delete_data(&mut self, where_data: HashMap<String, String>) -> bool {
        let mut deleted_column_index_set: HashSet<usize> = HashSet::new();
        for (column_key, column_val) in where_data.iter() {
            let column = self.search_column(column_key.to_string());
            let deleted_column_index = column.search_for_index(column_val.to_string());
            let mut current_deleted_column_index_set: HashSet<usize> = HashSet::new();
            deleted_column_index.iter().for_each(|i| {
                current_deleted_column_index_set.insert(*i);
            });
            if deleted_column_index_set.is_empty() {
                deleted_column_index_set = current_deleted_column_index_set;
            } else {
                // https://users.rust-lang.org/t/intersection-of-hashsets/32351/2
                deleted_column_index_set = deleted_column_index_set
                    .intersection(&current_deleted_column_index_set)
                    .copied()
                    .collect();
            }
            let mut index = 0;
            for i in deleted_column_index {
                // pengurangan dilakukan agar index yang terpilih sesuai dengan data yang dituju
                // karena setiap delete akan menggeser data yang ada
                self.delete(i - index);
                index += 1;
            }
        }
        true
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        let mut name_str_len = utils::integer_to_bytes(self.name.len() as isize).to_vec();
        bytes.append(&mut name_str_len);
        let mut name_bytes = utils::string_to_bytes(self.name.clone());
        bytes.append(&mut name_bytes);

        let mut column_len = utils::integer_to_bytes(self.columns.len() as isize).to_vec();
        bytes.append(&mut column_len);

        let mut table_len = utils::integer_to_bytes(self.length as isize).to_vec();
        bytes.append(&mut table_len);

        for column in self.columns.iter() {
            let mut col_bytes = column.to_bytes();
            bytes.append(&mut col_bytes);
        }
        bytes
    }

    pub fn to_data(buf_u8: &mut Vec<u8>) -> Table {
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
        let (table_column_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let table_column_len_u8: [u8; 8] = table_column_len.try_into().unwrap();
        let table_column_len_usize = usize::from_le_bytes(table_column_len_u8);

        *buf_u8 = rest_of_bytes.to_vec();
        // Parsing bytes to table length
        let rest_of_bytes = buf_u8.to_vec();
        let (table_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let table_len_u8: [u8; 8] = table_len.try_into().unwrap();
        let table_len_usize = usize::from_le_bytes(table_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Start Parsing the column
        let mut columns: Vec<Column> = Vec::new();

        for i in 0..table_column_len_usize {
            let new_col = Column::to_data(buf_u8);
            columns.push(new_col);
        }

        Table {
            name: name,
            length: table_len_usize,
            columns: columns,
        }
        // println!("{:?}", arr_values);
    }

    pub fn name_to_bytes(&self) -> Vec<u8> {
        let mut vec: Vec<u8> = Vec::new();
        for n in self.name.chars() {
            vec.push(n as u8);
        }
        return vec;
    }
}
