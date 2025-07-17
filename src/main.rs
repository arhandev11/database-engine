use std::{
    collections::{HashMap, HashSet},
    fs::File,
    future::pending,
    io::{ErrorKind, Write},
    result,
};

use crate::database::{
    column::Column,
    database_connection::DatabaseConnection,
    database_interface::DatabaseInterface,
    schema::Schema,
    table::{self, Table},
    test_interface::TestDatabaseInterface,
    utils::{self, DataType, InputDataEnum},
};
use zbus::{interface, Connection, Result};

mod database;

#[test]
fn test_dev() {
    // TestDatabaseInterface::test_show_database();
    // TestDatabaseInterface::test_create_database("articles".to_string());
    // TestDatabaseInterface::test_select_database("articles".to_string());
    // TestDatabaseInterface::test_drop_database("test-1".to_string());
    // TestDatabaseInterface::test_create_table_with_column("articles".to_string(), "users".to_string());
    // TestDatabaseInterface::test_create_table_with_column("articles".to_string(), "posts".to_string());
    // TestDatabaseInterface::test_drop_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_list_table("Test 2".to_string());
    // TestDatabaseInterface::test_add_column(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "text_aja".to_string(),
    //     "string".to_string(),
    // );
    // TestDatabaseInterface::test_list_column_on_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_delete_column_on_table(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "text_aja".to_string(),
    // );
    // TestDatabaseInterface::test_list_column_on_table("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_add_data("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_get_data("Test 2".to_string(), "table_1".to_string());
    // TestDatabaseInterface::test_search_data(
    //     "Test 2".to_string(),
    //     "table_1".to_string(),
    //     "first_name".to_string(),
    //     "Akbar".to_string(),
    // );
    // TestDatabaseInterface::test_delete_data("Test 2".to_string(), "table_1".to_string());

    // TestDatabaseInterface::test_update_data("articles".to_string(), "users".to_string());
    // TestDatabaseInterface::test_create_join_data();
    // TestDatabaseInterface::test_get_join_data("articles".to_string());
    // TestDatabaseInterface::print("articles".to_string());
}

#[test]
fn test() {
    // TestDatabaseInterface::test_create_database("articles".to_string());

    // TestDatabaseInterface::test_select_database("articles".to_string());

    // TestDatabaseInterface::test_create_table("articles".to_string(), "users".to_string());
    // TestDatabaseInterface::test_create_table("articles".to_string(), "posts".to_string());

    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "users".to_string(),
    //     "id".to_string(),
    //     "integer".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "users".to_string(),
    //     "first_name".to_string(),
    //     "string".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "users".to_string(),
    //     "last_name".to_string(),
    //     "string".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "posts".to_string(),
    //     "id".to_string(),
    //     "integer".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "posts".to_string(),
    //     "user_id".to_string(),
    //     "integer".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "posts".to_string(),
    //     "title".to_string(),
    //     "string".to_string(),
    // );
    // TestDatabaseInterface::test_add_column(
    //     "articles".to_string(),
    //     "posts".to_string(),
    //     "description".to_string(),
    //     "string".to_string(),
    // );

    // let data: Vec<HashMap<String, String>> = vec![
    //     HashMap::from([
    //         ("id".to_string(), "0".to_string()),
    //         ("first_name".to_string(), "Farhan".to_string()),
    //         ("last_name".to_string(), "Abdul".to_string()),
    //     ]),
    //     HashMap::from([
    //         ("id".to_string(), "1".to_string()),
    //         ("first_name".to_string(), "Akbar".to_string()),
    //         ("last_name".to_string(), "Maulana".to_string()),
    //     ]),
    //     HashMap::from([
    //         ("id".to_string(), "2".to_string()),
    //         ("first_name".to_string(), "Daffa".to_string()),
    //         ("last_name".to_string(), "Haryadi".to_string()),
    //     ]),
    //     HashMap::from([
    //         ("id".to_string(), "3".to_string()),
    //         ("first_name".to_string(), "Hanif".to_string()),
    //         ("last_name".to_string(), "Ramadhan".to_string()),
    //     ]),
    //     HashMap::from([
    //         ("id".to_string(), "4".to_string()),
    //         ("first_name".to_string(), "Rudiansyah".to_string()),
    //         ("last_name".to_string(), "Wijaya".to_string()),
    //     ]),
    // ];
    // for item in data {
    //     TestDatabaseInterface::test_add_data("articles".to_string(), "users".to_string(), item);
    // }
    // let data: Vec<HashMap<String, String>> = vec![
    //         HashMap::from([
    //             ("id".to_string(), "0".to_string()),
    //             ("user_id".to_string(), "0".to_string()),
    //             ("title".to_string(), "Judul 0".to_string()),
    //             ("description".to_string(), "Maxime fugit voluptatem dolor et qui voluptate. Atque veritatis velit modi reiciendis rerum. Magni voluptate laudantium ipsam est vero expedita aspernatur cum. Quas neque dolores sapiente sequi et velit quia sapiente.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "1".to_string()),
    //             ("user_id".to_string(), "0".to_string()),
    //             ("title".to_string(), "Judul 1".to_string()),
    //             ("description".to_string(), "Consequatur aut deserunt libero voluptas beatae recusandae excepturi libero. Quas dolorem impedit et deserunt. Consequuntur expedita ipsam delectus qui consequuntur sunt dolorem.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "2".to_string()),
    //             ("user_id".to_string(), "1".to_string()),
    //             ("title".to_string(), "Judul 2".to_string()),
    //             ("description".to_string(), "Sed architecto consequuntur rerum beatae. Inventore et porro ullam omnis eos nam sit id. Provident ducimus explicabo sed nostrum quia laudantium quia.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "3".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 3".to_string()),
    //             ("description".to_string(), "Ut quo aut debitis totam. Et laborum aperiam et maiores est doloremque ut hic. Quo et nesciunt reprehenderit velit.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "4".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 4".to_string()),
    //             ("description".to_string(), "Harum quo iste illo quaerat. Omnis aliquid deleniti magnam optio. Fuga sint vitae rerum harum. Eum quae laboriosam dolorem distinctio quidem corrupti.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "5".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 5".to_string()),
    //             ("description".to_string(), "Ut et eos eos suscipit. Cum exercitationem aut aperiam illum. Iure iste sit illo recusandae est.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "6".to_string()),
    //             ("user_id".to_string(), "3".to_string()),
    //             ("title".to_string(), "Judul 6".to_string()),
    //             ("description".to_string(), "Totam quidem cum reprehenderit rerum et consequatur soluta. Repudiandae dolores assumenda sed ex quo. Aspernatur quis perspiciatis omnis tempore optio fugiat. Maiores expedita quaerat odit minus nulla. Et reprehenderit repellendus eos temporibus reprehenderit nam libero.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "7".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 7".to_string()),
    //             ("description".to_string(), "Tenetur libero dolorem voluptatem incidunt laudantium. Doloremque rem ut autem itaque nihil labore itaque. Ipsa vel voluptate sint expedita.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "8".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 8".to_string()),
    //             ("description".to_string(), "Et minima ex tempore voluptatem aliquam et quibusdam. Odit illum quam nemo eveniet. Illum laborum quos atque sapiente aut at. Voluptatem explicabo et sit pariatur laborum consequatur.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "9".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 9".to_string()),
    //             ("description".to_string(), "Et possimus eum aliquam. Sit molestiae recusandae itaque odio eius. Facilis asperiores minima enim atque et harum. Nostrum voluptatem voluptas esse ut voluptatem omnis repudiandae culpa. Officia dolore quia facilis culpa.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "10".to_string()),
    //             ("user_id".to_string(), "1".to_string()),
    //             ("title".to_string(), "Judul 10".to_string()),
    //             ("description".to_string(), "Cum et autem ut laudantium eum quas. Veritatis molestias aliquam est ut labore voluptas omnis. At velit consequatur perferendis porro voluptas autem.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "11".to_string()),
    //             ("user_id".to_string(), "1".to_string()),
    //             ("title".to_string(), "Judul 11".to_string()),
    //             ("description".to_string(), "Lorem ipsum dolor sit amet consectetur, adipisicing elit. Minus voluptatem vel excepturi eos! Consectetur, reprehenderit harum dolor sed pariatur reiciendis non minima possimus quaerat numquam deleniti consequatur placeat aliquid rem.".to_string()),
    //         ]),
    //         HashMap::from([
    //             ("id".to_string(), "12".to_string()),
    //             ("user_id".to_string(), "2".to_string()),
    //             ("title".to_string(), "Judul 12".to_string()),
    //             ("description".to_string(), "Aut sunt excepturi facere a at molestiae recusandae sed. Rerum rerum occaecati expedita magnam aut voluptas modi. Dolorem laboriosam asperiores itaque voluptatum impedit. Ad velit velit aperiam molestiae nemo ut.".to_string()),
    //         ]),
    //     ];
    //     for item in data {
    //         TestDatabaseInterface::test_add_data("articles".to_string(), "posts".to_string(), item);
    //     }

    // TestDatabaseInterface::test_get_data("articles".to_string(), "users".to_string());
    // TestDatabaseInterface::test_get_data("articles".to_string(), "posts".to_string());

    // let where_data = HashMap::from([
    //     ("id".to_string(), "0".to_string()),
    //     ("first_name".to_string(), "Farhan".to_string()),
    // ]);
    // let updated_data = HashMap::from([("last_name".to_string(), "Abdul Hamid".to_string())]);
    // TestDatabaseInterface::test_update_data("articles".to_string(), "users".to_string(), where_data, updated_data);

    // TestDatabaseInterface::test_search_data("articles".to_string(), "users".to_string(), "id".to_string(), "0".to_string());

    // TestDatabaseInterface::test_join_table(
    //     "articles".to_string(),
    //     "users".to_string(),
    //     "id".to_string(),
    //     "posts".to_string(),
    //     "user_id".to_string(),
    //     "right".to_string(),
    // );

    // let where_data = HashMap::from([
    //     ("first_name".to_string(), "Rudiansyah".to_string()),
    // ]);

    // TestDatabaseInterface::test_delete_data("articles".to_string(), "users".to_string(), where_data);
    
    TestDatabaseInterface::print("library".to_string());
    let res = utils::integer_to_bytes(10000000000 as isize).to_vec();
    println!("{:?}", res);

}

#[tokio::main]
async fn main() -> Result<()> {
    let database_connection: DatabaseConnection = DatabaseConnection {
        db_interface: DatabaseInterface {
            is_connect: false,
            database: None,
        },
    };

    let connection: Connection = Connection::session().await?;

    connection
        .object_server()
        .at("/org/two/DatabaseConnection", database_connection)
        .await?;

    connection
        .request_name("org.two.DatabaseConnection")
        .await?;

    loop {
        pending::<()>().await;
    }
}
