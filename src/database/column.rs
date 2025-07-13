use std::{
    collections::HashMap,
    fmt::format,
    fs::File,
    io::{Read, Write},
    result,
};

use crate::database::{
    cell::Cell,
    utils::{self, bytes_to_string, DataType, InputDataEnum},
};

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub rows: Vec<Cell>,
}

impl Column {
    // fn get_name(&self) -> String {
    //     self.name.clone()
    // }
    // fn get_type(&self) -> DataType {
    //     self.data_type
    // }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_data_type(&self) -> DataType {
        match self.data_type {
            DataType::String => DataType::String,
            DataType::Integer => DataType::Integer,
            DataType::Null => DataType::Null,
        }
    }

    pub fn print_column(&self) {
        for i in &self.rows {
            i.print();
        }
    }

    pub fn insert_data(&mut self, input: InputDataEnum) {
        let data_type = match self.data_type {
            DataType::Integer => DataType::Integer,
            DataType::String => DataType::String,
            DataType::Null => DataType::Null,
        };
        let result = match input {
            InputDataEnum::String(word) => utils::string_to_bytes(word),
            InputDataEnum::Integer(number) => utils::integer_to_bytes(number).to_vec(),
            InputDataEnum::Null => [0; (isize::BITS / 8) as usize].to_vec(),
        };
        let new_cell = Cell {
            data_type: data_type,
            data_value: result,
        };
        self.rows.push(new_cell);
    }

    pub fn update_data(&mut self, index: usize, input: &InputDataEnum) {
        let byte = match input {
            InputDataEnum::String(word) => utils::string_to_bytes(word.to_string()),
            InputDataEnum::Integer(num) => utils::integer_to_bytes(*num).to_vec(),
            InputDataEnum::Null => vec![],
        };

        self.rows[index].change_value(byte);
    }

    pub fn delete_data(&mut self, index: usize) {
        self.rows.remove(index);
    }

    fn extract_data(&mut self) {}

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();
        // Parsing columns row length
        let column_data_length = utils::integer_to_bytes(self.rows.len() as isize);
        result.append(&mut column_data_length.to_vec());

        // Parsing Columns name
        let mut column_name_str_len = utils::integer_to_bytes(self.name.len() as isize).to_vec();
        result.append(&mut column_name_str_len);
        let mut column_name_bytes = utils::string_to_bytes(self.name.clone());
        result.append(&mut column_name_bytes);

        match self.data_type {
            DataType::String => {
                result.push(0);
            }
            DataType::Integer => {
                result.push(1);
            }
            DataType::Null => {}
        };

        for column_data in &self.rows {
            match column_data.data_type {
                DataType::String => {
                    let mut str_len =
                        utils::integer_to_bytes(column_data.data_value.len() as isize).to_vec();
                    result.append(&mut str_len);
                }
                DataType::Integer => {}
                DataType::Null => {}
            };
            result.append(&mut column_data.data_value.clone());
            // bytes.append(&mut column_data.);
        }
        result
    }

    pub fn to_data(buf_u8: &mut Vec<u8>) -> Column {
        // Parsing bytes to row length first
        let rest_of_bytes = buf_u8.to_vec();
        let (col_row_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let col_row_len_u8: [u8; 8] = col_row_len.try_into().unwrap();
        let col_row_len_usize = usize::from_le_bytes(col_row_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Parsing bytes to column name length first
        let rest_of_bytes = buf_u8.to_vec();
        let (col_name_len, rest_of_bytes) = rest_of_bytes.split_at((usize::BITS / 8) as usize);
        let col_name_len_u8: [u8; 8] = col_name_len.try_into().unwrap();
        let col_name_len_usize = usize::from_le_bytes(col_name_len_u8);
        *buf_u8 = rest_of_bytes.to_vec();

        // Parsing bytes to column name value first
        let rest_of_bytes = buf_u8.to_vec();
        let (col_name_u8, rest_of_bytes) = rest_of_bytes.split_at(col_name_len_usize);
        let col_name = bytes_to_string(col_name_u8.to_vec());
        *buf_u8 = rest_of_bytes.to_vec();

        // Parsing Data Type
        let rest_of_bytes = buf_u8.to_vec();
        let (data_type, rest_of_bytes) = rest_of_bytes.split_at(1);
        *buf_u8 = rest_of_bytes.to_vec();

        let mut column = match data_type[0] {
            0 => Column {
                data_type: DataType::String,
                name: col_name,
                rows: Vec::new(),
            },
            1 => Column {
                data_type: DataType::Integer,
                name: col_name,
                rows: Vec::new(),
            },
            _ => {
                panic!("Something went wrong on parsing bytes to table")
            }
        };

        for i in 0..col_row_len_usize {
            match data_type[0] {
                0 => {
                    let rest_of_bytes = buf_u8.to_vec();
                    let (cell_len, rest_of_bytes) =
                        rest_of_bytes.split_at((usize::BITS / 8) as usize);
                    let cell_len_u8: [u8; 8] = cell_len.try_into().unwrap();
                    let cell_len_usize = usize::from_le_bytes(cell_len_u8);
                    *buf_u8 = rest_of_bytes.to_vec();

                    // Parsing bytes to table cell value first
                    let rest_of_bytes = buf_u8.to_vec();
                    let (cell_u8, rest_of_bytes) = rest_of_bytes.split_at(cell_len_usize);
                    let new_cell = Cell {
                        data_type: DataType::String,
                        data_value: cell_u8.to_vec(),
                    };
                    column.rows.push(new_cell);
                    *buf_u8 = rest_of_bytes.to_vec();
                }
                1 => {
                    // Parsing bytes to table cell value first
                    let rest_of_bytes = buf_u8.to_vec();
                    let (cell_u8, rest_of_bytes) =
                        rest_of_bytes.split_at((usize::BITS / 8) as usize);
                    let new_cell = Cell {
                        data_type: DataType::Integer,
                        data_value: cell_u8.to_vec(),
                    };
                    column.rows.push(new_cell);
                    *buf_u8 = rest_of_bytes.to_vec();
                }
                _ => {
                    panic!("Something went wrong on parsing bytes to table")
                }
            };
        }
        column
    }
}
