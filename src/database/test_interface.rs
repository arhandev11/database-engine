use std::collections::HashMap;

use crate::database::{
    column::Column,
    database_interface::DatabaseInterface,
    schema::Schema,
    table::Table,
    utils::{DataType, InputDataEnum},
};

fn test_1() -> std::io::Result<()> {
    let mut table = Table {
        name: String::from("table_1"),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "column_1".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    let column_2 = Column {
        name: "Column 2".to_string(),
        data_type: DataType::Integer,
        rows: Vec::new(),
    };

    table.add_column(column_1);
    table.add_column(column_2);

    let string_1 = InputDataEnum::String("Data String 1".to_string());
    let int_1 = InputDataEnum::Integer(25);
    let data_1 = vec![string_1, int_1];

    let string_2 = InputDataEnum::String("Data String 2".to_string());
    let int_2 = InputDataEnum::Integer(30);
    let data_2 = vec![string_2, int_2];

    table.add_data(data_1);
    table.add_data(data_2);

    // table.print();
    let mut buf_result = table.to_bytes();

    let decoded_table = Table::to_data(&mut buf_result);

    decoded_table.print();

    println!("{:?}", buf_result);

    // let mut file = File::create("output")?;
    // file.write_all(&buf_result)?;

    Ok(())
}

fn test_2() {
    let mut table = Table {
        name: String::from("table_1"),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "column_1".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    table.add_column(column_1);

    let cell = InputDataEnum::String("Hello World".to_string());
    table.add_data_column("column_1".to_string(), cell);

    let cell = InputDataEnum::String("Hello World 2".to_string());
    table.add_data_column("column_1".to_string(), cell);

    let res = table.get_data();
    println!("{:?}", res);

    let map: HashMap<String, InputDataEnum> = HashMap::from([(
        "column_1".to_string(),
        InputDataEnum::String("Test Update".to_string()),
    )]);
    table.update(0, map);
    let res = table.get_data();
    println!("{:?}", res);

    table.delete(0);
    let res = table.get_data();
    println!("{:?}", res);
}

fn test_3() -> std::io::Result<()> {
    // ================= Table 1 =========================
    let mut table_1 = Table {
        name: String::from("users"),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "name".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    let column_2 = Column {
        name: "wallet_id".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    table_1.add_column(column_1);
    table_1.add_column(column_2);

    let cell_1 = InputDataEnum::String("Farhan".to_string());
    let cell_2 = InputDataEnum::String("1".to_string());
    let data = vec![cell_1, cell_2];
    table_1.add_data(data);

    let cell_1 = InputDataEnum::String("Umar".to_string());
    let cell_2 = InputDataEnum::String("2".to_string());
    let data = vec![cell_1, cell_2];
    table_1.add_data(data);

    // ================= Table 2 =========================
    let mut table_2 = Table {
        name: String::from("wallets"),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "saldo".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    let column_2 = Column {
        name: "id".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    table_2.add_column(column_2);
    table_2.add_column(column_1);

    let cell_1 = InputDataEnum::String("1".to_string());
    let cell_2 = InputDataEnum::String("50000".to_string());
    let data = vec![cell_1, cell_2];
    table_2.add_data(data);

    let cell_1 = InputDataEnum::String("3".to_string());
    let cell_2 = InputDataEnum::String("10000".to_string());
    let data = vec![cell_1, cell_2];
    table_2.add_data(data);

    // let res = table.get_data();

    let mut schema = Schema {
        name: "test".to_string(),
        tables: Vec::new(),
    };
    schema.add_table(table_1);
    schema.add_table(table_2);

    let res = schema.get_data_join(
        "users".to_string(),
        "wallet_id".to_string(),
        "wallets".to_string(),
        "id".to_string(),
    )?;
    println!("{:?}", res);

    Ok(())
}

fn test_4() -> std::io::Result<()> {
    let mut schema = Schema {
        name: "test".to_string(),
        tables: Vec::new(),
    };
    let result = schema.get_data_join(
        "users".to_string(),
        "wallet_id".to_string(),
        "wallets".to_string(),
        "id".to_string(),
    )?;

    println!("{:?}", result);

    Ok(())
}

fn test_column() -> std::io::Result<()> {
    let mut column_1 = Column {
        name: "column_1".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };

    let string_1 = InputDataEnum::String("Data String 1".to_string());
    let string_2 = InputDataEnum::String("Data String 2".to_string());

    column_1.insert_data(string_1);
    column_1.insert_data(string_2);

    let mut byte = column_1.to_bytes();

    println!("{:?}", byte);

    let decoded_column = Column::to_data(&mut byte);

    decoded_column.print_column();
    println!("{:?}", byte);
    Ok(())
}

fn create_test_table(table_name: &str) -> Table {
    let mut table = Table {
        name: String::from(table_name.to_string()),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "column_1".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    let column_2 = Column {
        name: "Column 2".to_string(),
        data_type: DataType::Integer,
        rows: Vec::new(),
    };
    table.add_column(column_1);
    table.add_column(column_2);
    let string_1 = InputDataEnum::String("Data String 1".to_string());
    let int_1 = InputDataEnum::Integer(25);
    let data_1 = vec![string_1, int_1];
    let string_2 = InputDataEnum::String("Data String 2".to_string());
    let int_2 = InputDataEnum::Integer(30);
    let data_2 = vec![string_2, int_2];
    table.add_data(data_1);
    table.add_data(data_2);

    table
}

fn test_schema() -> std::io::Result<()> {
    let table_1 = create_test_table("Table_1");
    let table_2 = create_test_table("Table_2");
    let table_3 = create_test_table("Table_3");
    let mut tables: Vec<Table> = Vec::new();
    tables.push(table_1);
    tables.push(table_2);
    tables.push(table_3);

    let database = Schema {
        name: "new_database".to_string(),
        tables,
    };

    let mut buf_result = database.to_bytes();

    let decoded_database = Schema::to_data(&mut buf_result);

    decoded_database.print();
    println!("{:?}", decoded_database.tables.len());
    println!("{:?}", buf_result);

    Ok(())
    // let mut file = File::create("output")?;
    // file.write_all(&buf_result)?;
}

fn test_table() -> std::io::Result<()> {
    let mut table = Table {
        name: String::from("table_1"),
        columns: Vec::new(),
        length: 0,
    };
    let column_1 = Column {
        name: "column_1".to_string(),
        data_type: DataType::String,
        rows: Vec::new(),
    };
    let column_2 = Column {
        name: "Column 2".to_string(),
        data_type: DataType::Integer,
        rows: Vec::new(),
    };

    table.add_column(column_1);
    table.add_column(column_2);
    let string_1 = InputDataEnum::String("Data String 1".to_string());
    let int_1 = InputDataEnum::Integer(25);
    let data_1 = vec![string_1, int_1];

    let string_2 = InputDataEnum::String("Data String 2".to_string());
    let int_2 = InputDataEnum::Integer(30);
    let data_2 = vec![string_2, int_2];

    table.add_data(data_1);
    table.add_data(data_2);

    let mut buf_result = table.to_bytes();

    let decoded_table = Table::to_data(&mut buf_result);

    decoded_table.print();
    println!("{:?}", buf_result);

    Ok(())
    // let mut file = File::create("output")?;
    // file.write_all(&buf_result)?;
}

pub struct TestDatabaseInterface {}

impl TestDatabaseInterface {
    pub fn test_select_database(database_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.select_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terhubung")
            }
            false => println!("Database tidak ditemukan"),
        };
    }

    pub fn test_create_database(database_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.create_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terbuat")
            }
            false => println!("Terjadi suatu error"),
        };
    }

    pub fn test_drop_database(database_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.drop_database(&database_name);
        match res {
            true => {
                println!("Database berhasil dihapus")
            }
            false => println!("Database tidak ditemukan"),
        };
    }

    pub fn test_list_table(database_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.select_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terhubung")
            }
            false => println!("Database tidak ditemukan"),
        };

        let res_tables = db_interface.list_all_table();
        println!("{:?}", res_tables);
    }

    pub fn test_create_table(database_name: String, table_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.select_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terhubung")
            }
            false => println!("Database tidak ditemukan"),
        };
        let columns = vec![];
        let res_tables = db_interface.create_table(&table_name, columns);
        println!("{:?}", res_tables);
    }

    pub fn test_create_table_with_column(database_name: String, table_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.select_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terhubung")
            }
            false => println!("Database tidak ditemukan"),
        };
        let mut column1: HashMap<String, String> = HashMap::new();
        column1.insert("name".to_string(), "id".to_string());
        column1.insert("type".to_string(), "integer".to_string());
        let mut column2: HashMap<String, String> = HashMap::new();
        column2.insert("name".to_string(), "first_name".to_string());
        column2.insert("type".to_string(), "string".to_string());
        let mut column3: HashMap<String, String> = HashMap::new();
        column3.insert("name".to_string(), "last_name".to_string());
        column3.insert("type".to_string(), "string".to_string());
        let mut columns = vec![];
        columns.push(column1);
        columns.push(column2);
        columns.push(column3);
        let res_tables = db_interface.create_table(&table_name, columns);
        println!("{:?}", res_tables);
    }


    pub fn test_drop_table(database_name: String, table_name: String) {
        let mut db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.select_database(&database_name);
        match res {
            true => {
                println!("Database berhasil terhubung")
            }
            false => println!("Database tidak ditemukan"),
        };

        db_interface.drop_table(&table_name);
    }
}
