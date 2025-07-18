use std::collections::HashMap;

use crate::database::{
    column::Column,
    database_interface::DatabaseInterface,
    schema::Schema,
    table::Table,
    utils::{parse_new_column, DataType, InputDataEnum},
};

pub struct TestDatabaseInterface {}

impl TestDatabaseInterface {
    pub fn test_show_database() -> Vec<String> {
        let db_interface = DatabaseInterface {
            is_connect: false,
            database: None,
        };
        let res = db_interface.show_databases();
        println!("{:?}", res);
        res
    }
    pub fn print(database_name: String) {
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

        let res_columns = db_interface.print();
        println!("{:?}", res_columns);
    }

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
        match res_tables {
            true => {
                println!("Berhasil membuat tabel")
            }
            _ => {
                panic!("Something went wrong")
            }
        };
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

    pub fn test_add_column(
        database_name: String,
        table_name: String,
        name: String,
        data_type: String,
    ) {
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

        let res = db_interface.add_column_to_table(&table_name, name, data_type);
        match res {
            true => {
                println!("Berhasil membuat column")
            }
            _ => {
                panic!("Something went wrong")
            }
        };
    }

    pub fn test_list_column_on_table(database_name: String, table_name: String) {
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

        let res_columns = db_interface.list_column_on_table(table_name);
        println!("{:?}", res_columns);
    }

    pub fn test_delete_column_on_table(
        database_name: String,
        table_name: String,
        column_name: String,
    ) {
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

        let res_columns = db_interface.delete_column_on_table(table_name, column_name);
        println!("{:?}", res_columns);
    }

    pub fn test_add_data(database_name: String, table_name: String, data: HashMap<String, String>) {
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

        let res = db_interface.add_data(&table_name, data);
        match res {
            true => {
                println!("Berhasil membuat data")
            }
            _ => {
                panic!("Something went wrong")
            }
        };
    }

    pub fn test_get_data(database_name: String, table_name: String) {
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

        db_interface.get_data(&table_name);
    }

    pub fn test_search_data(
        database_name: String,
        table_name: String,
        column_name: String,
        value: String,
    ) {
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

        db_interface.search_data(&table_name, column_name, value);
    }

    pub fn test_update_data(
        database_name: String,
        table_name: String,
        where_data: HashMap<String, String>,
        updated_data: HashMap<String, String>,
    ) {
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

        let res = db_interface.update_data(&table_name, where_data, updated_data);
        match res {
            true => {
                println!("Berhasil mengubah data")
            }
            false => panic!("Gagal mengubah data"),
        };
    }

    pub fn test_delete_data(
        database_name: String,
        table_name: String,
        where_data: HashMap<String, String>,
    ) {
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

        let res = db_interface.delete_data(&table_name, where_data);
        match res {
            true => {
                println!("Berhasil menghapus data")
            }
            false => panic!("Gagal menghapus data"),
        };
    }

    pub fn test_create_join_data() {
        TestDatabaseInterface::test_create_database("articles".to_string());
        TestDatabaseInterface::test_create_table("articles".to_string(), "users".to_string());
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "users".to_string(),
            "id".to_string(),
            "integer".to_string(),
        );
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "users".to_string(),
            "first_name".to_string(),
            "string".to_string(),
        );
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "users".to_string(),
            "last_name".to_string(),
            "string".to_string(),
        );

        let data: Vec<HashMap<String, String>> = vec![
            HashMap::from([
                ("id".to_string(), "0".to_string()),
                ("first_name".to_string(), "Farhan".to_string()),
                ("last_name".to_string(), "Abdul".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "1".to_string()),
                ("first_name".to_string(), "Akbar".to_string()),
                ("last_name".to_string(), "Maulana".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "2".to_string()),
                ("first_name".to_string(), "Daffa".to_string()),
                ("last_name".to_string(), "Haryadi".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "3".to_string()),
                ("first_name".to_string(), "Hanif".to_string()),
                ("last_name".to_string(), "Ramadhan".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "4".to_string()),
                ("first_name".to_string(), "Rudiansyah".to_string()),
                ("last_name".to_string(), "Wijaya".to_string()),
            ]),
        ];
        for item in data {
            TestDatabaseInterface::test_add_data("articles".to_string(), "users".to_string(), item);
        }

        TestDatabaseInterface::test_create_table("articles".to_string(), "posts".to_string());
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "posts".to_string(),
            "id".to_string(),
            "integer".to_string(),
        );
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "posts".to_string(),
            "user_id".to_string(),
            "integer".to_string(),
        );
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "posts".to_string(),
            "title".to_string(),
            "string".to_string(),
        );
        TestDatabaseInterface::test_add_column(
            "articles".to_string(),
            "posts".to_string(),
            "description".to_string(),
            "string".to_string(),
        );

        let data: Vec<HashMap<String, String>> = vec![
            HashMap::from([
                ("id".to_string(), "0".to_string()),
                ("user_id".to_string(), "0".to_string()),
                ("title".to_string(), "Judul 0".to_string()),
                ("description".to_string(), "Maxime fugit voluptatem dolor et qui voluptate. Atque veritatis velit modi reiciendis rerum. Magni voluptate laudantium ipsam est vero expedita aspernatur cum. Quas neque dolores sapiente sequi et velit quia sapiente.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "1".to_string()),
                ("user_id".to_string(), "0".to_string()),
                ("title".to_string(), "Judul 1".to_string()),
                ("description".to_string(), "Consequatur aut deserunt libero voluptas beatae recusandae excepturi libero. Quas dolorem impedit et deserunt. Consequuntur expedita ipsam delectus qui consequuntur sunt dolorem.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "2".to_string()),
                ("user_id".to_string(), "1".to_string()),
                ("title".to_string(), "Judul 2".to_string()),
                ("description".to_string(), "Sed architecto consequuntur rerum beatae. Inventore et porro ullam omnis eos nam sit id. Provident ducimus explicabo sed nostrum quia laudantium quia.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "3".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 3".to_string()),
                ("description".to_string(), "Ut quo aut debitis totam. Et laborum aperiam et maiores est doloremque ut hic. Quo et nesciunt reprehenderit velit.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "4".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 4".to_string()),
                ("description".to_string(), "Harum quo iste illo quaerat. Omnis aliquid deleniti magnam optio. Fuga sint vitae rerum harum. Eum quae laboriosam dolorem distinctio quidem corrupti.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "5".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 5".to_string()),
                ("description".to_string(), "Ut et eos eos suscipit. Cum exercitationem aut aperiam illum. Iure iste sit illo recusandae est.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "6".to_string()),
                ("user_id".to_string(), "3".to_string()),
                ("title".to_string(), "Judul 6".to_string()),
                ("description".to_string(), "Totam quidem cum reprehenderit rerum et consequatur soluta. Repudiandae dolores assumenda sed ex quo. Aspernatur quis perspiciatis omnis tempore optio fugiat. Maiores expedita quaerat odit minus nulla. Et reprehenderit repellendus eos temporibus reprehenderit nam libero.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "7".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 7".to_string()),
                ("description".to_string(), "Tenetur libero dolorem voluptatem incidunt laudantium. Doloremque rem ut autem itaque nihil labore itaque. Ipsa vel voluptate sint expedita.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "8".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 8".to_string()),
                ("description".to_string(), "Et minima ex tempore voluptatem aliquam et quibusdam. Odit illum quam nemo eveniet. Illum laborum quos atque sapiente aut at. Voluptatem explicabo et sit pariatur laborum consequatur.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "9".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 9".to_string()),
                ("description".to_string(), "Et possimus eum aliquam. Sit molestiae recusandae itaque odio eius. Facilis asperiores minima enim atque et harum. Nostrum voluptatem voluptas esse ut voluptatem omnis repudiandae culpa. Officia dolore quia facilis culpa.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "10".to_string()),
                ("user_id".to_string(), "1".to_string()),
                ("title".to_string(), "Judul 10".to_string()),
                ("description".to_string(), "Cum et autem ut laudantium eum quas. Veritatis molestias aliquam est ut labore voluptas omnis. At velit consequatur perferendis porro voluptas autem.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "11".to_string()),
                ("user_id".to_string(), "1".to_string()),
                ("title".to_string(), "Judul 11".to_string()),
                ("description".to_string(), "Lorem ipsum dolor sit amet consectetur, adipisicing elit. Minus voluptatem vel excepturi eos! Consectetur, reprehenderit harum dolor sed pariatur reiciendis non minima possimus quaerat numquam deleniti consequatur placeat aliquid rem.".to_string()),
            ]),
            HashMap::from([
                ("id".to_string(), "12".to_string()),
                ("user_id".to_string(), "2".to_string()),
                ("title".to_string(), "Judul 12".to_string()),
                ("description".to_string(), "Aut sunt excepturi facere a at molestiae recusandae sed. Rerum rerum occaecati expedita magnam aut voluptas modi. Dolorem laboriosam asperiores itaque voluptatum impedit. Ad velit velit aperiam molestiae nemo ut.".to_string()),
            ]),
        ];
        for item in data {
            TestDatabaseInterface::test_add_data("articles".to_string(), "posts".to_string(), item);
        }
    }

    pub fn test_join_table(
        database_name: String,
        table_name: String,
        column_name: String,
        table_join: String,
        column_join: String,
        join_type: String,
    ) {
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
        db_interface.join_table(table_name, column_name, table_join, column_join, join_type);
    }
}
