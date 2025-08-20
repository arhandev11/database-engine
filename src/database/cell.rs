use std::{
    collections::HashMap,
    fmt::format,
    fs::File,
    io::{Read, Write},
    result,
};

use crate::database::utils::{self, DataType, InputDataEnum};

pub struct Cell {
    pub data_type: DataType,
    pub data_value: Vec<u8>,
}

impl Cell {
    pub fn print(&self) {
        match self.data_type {
            DataType::String => {
                let result = utils::bytes_to_string(self.data_value.clone());
                println!("{:?}", result);
            }
            DataType::Integer => {
                let result = utils::bytes_to_integer(self.data_value.clone());
                println!("{:?}", result);
            }
            DataType::Null => {
                println!("NULL");
            }
        }
    }

    // https://users.rust-lang.org/t/returning-different-types-in-match-arms/73508/3
    pub fn value(&self) -> InputDataEnum {
        match self.data_type {
            DataType::String => InputDataEnum::String(utils::bytes_to_string(self.data_value.clone())),
            DataType::Integer => InputDataEnum::Integer(utils::bytes_to_integer(self.data_value.clone())),
            DataType::Null => InputDataEnum::Null,
        }
    }

    pub fn change_value(&mut self, data: Vec<u8>) {
        self.data_value = data;
    }
}
