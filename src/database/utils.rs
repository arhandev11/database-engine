use serde::{Deserialize, Serialize};

use crate::database::{cell::Cell, column::Column, table::Table, utils};

pub enum DataType {
    String,
    Integer,
    Null,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InputDataEnum {
    String(String),
    Integer(isize),
    Null,
}

pub fn bytes_to_string(arr_vec: Vec<u8>) -> String {
    let mut res_str = "".to_string();
    for i in &arr_vec {
        res_str.push(*i as char);
    }
    return res_str;
}

pub fn string_to_bytes(word: String) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::new();
    for n in word.chars() {
        vec.push(n as u8);
    }
    return vec;
}

pub fn integer_to_bytes(num: isize) -> [u8; (isize::BITS / 8) as usize] {
    let bytes: [u8; (isize::BITS / 8) as usize] = isize::to_le_bytes(num);
    return bytes;
}

fn string_array_to_bytes(words: Vec<String>) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::new();
    for item in words {
        let len_bytes = utils::integer_to_bytes(item.len() as isize);
        vec.append(&mut len_bytes.to_vec());
        let mut byte_string = utils::string_to_bytes(item);
        vec.append(&mut byte_string);
    }
    vec
}

fn integer_array_to_bytes(numbers: Vec<isize>) -> Vec<u8> {
    let mut vec: Vec<u8> = Vec::new();
    for item in numbers {
        let len_bytes = utils::integer_to_bytes(item);
        vec.append(&mut len_bytes.to_vec());
    }
    return vec;
}

pub fn bytes_to_integer(arr_vec: Vec<u8>) -> isize {
    let number = isize::from_le_bytes(arr_vec.try_into().unwrap());
    return number;
}

fn bytes_to_integer_array(buf_str: &mut Vec<u8>, length: usize) -> Vec<isize> {
    let mut arr_int = vec![];
    for j in 0..length {
        let (word_len, arr_values) = buf_str.split_at((usize::BITS / 8) as usize);
        println!("{:?}", word_len);
        let word_len_u8: [u8; 8] = word_len.try_into().unwrap();
        let word_len_usize = usize::from_le_bytes(word_len_u8);
        let (current, rest_0f_values) = arr_values.split_at(word_len_usize);
        let current_arr_buff = current.to_vec();
        *buf_str = rest_0f_values.to_vec();
        let string_result = bytes_to_integer(current_arr_buff);
        arr_int.push(string_result);
    }

    return arr_int;
}

fn bytes_to_string_array(buf_str: &mut Vec<u8>, length: usize) -> Vec<String> {
    let mut arr_str = vec![];
    for j in 0..length {
        let (word_len, arr_values) = buf_str.split_at((usize::BITS / 8) as usize);
        println!("{:?}", word_len);
        let word_len_u8: [u8; 8] = word_len.try_into().unwrap();
        let word_len_usize = usize::from_le_bytes(word_len_u8);
        let (current, rest_0f_values) = arr_values.split_at(word_len_usize);
        let current_arr_buff = current.to_vec();
        *buf_str = rest_0f_values.to_vec();
        let string_result = bytes_to_string(current_arr_buff);
        arr_str.push(string_result);
    }

    return arr_str;
}


