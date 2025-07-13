use std::collections::HashMap;

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
    ) -> HashMap<String, InputDataEnum> {
        let mut result: HashMap<String, InputDataEnum> = HashMap::new();
        let selected_column = self.search_column(column_name);
        for index in 0..self.length {
            let check: bool = match selected_column.rows[index].value() {
                InputDataEnum::String(word) => {
                    if search == word {
                        result = self.get_data_by_index(index);
                        true
                    } else {
                        false
                    }
                }
                InputDataEnum::Integer(num) => {
                    if search.as_str().parse::<isize>().unwrap() == num {
                        result = self.get_data_by_index(index);
                        true
                    } else {
                        false
                    }
                }
                InputDataEnum::Null => false,
            };
            if check == true {
                break;
            }
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
        self.columns.push(column);
    }

    pub fn add_data_column(&mut self, name: String, input: InputDataEnum) {
        for column in &mut self.columns {
            if column.name == name {
                column.insert_data(input);
                self.length += 1;
                break;
            }
        }
    }

    pub fn add_data(&mut self, input_data: Vec<InputDataEnum>) {
        let mut index = 0;
        for item in input_data.into_iter() {
            self.columns[index].insert_data(item);
            index += 1;
        }
        self.length += 1;
    }

    pub fn update(&mut self, index: usize, map: HashMap<String, InputDataEnum>) {
        for (key, value) in map.iter() {
            for column in &mut self.columns {
                if column.name == *key {
                    column.update_data(index, value);
                    break;
                }
            }
        }
    }

    pub fn delete_column(&mut self, column_name: String){

    }

    pub fn delete(&mut self, index: usize) {
        for col in &mut self.columns {
            col.delete_data(index);
        }
        self.length -= 1;
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        let mut name_str_len = utils::integer_to_bytes(self.name.len() as isize).to_vec();
        bytes.append(&mut name_str_len);
        let mut name_bytes = utils::string_to_bytes(self.name.clone());
        bytes.append(&mut name_bytes);

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
        let (table_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let table_len_u8: [u8; 8] = table_len.try_into().unwrap();
        let table_len_usize = usize::from_le_bytes(table_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Start Parsing the column
        let mut columns: Vec<Column> = Vec::new();

        for i in 0..table_len_usize {
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
