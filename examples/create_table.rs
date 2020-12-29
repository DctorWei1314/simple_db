use btree::{wwcdb, DATATYPE};
use btree::data_item::{Data_item, Data_item_info};
use btree::db_str::Db_str;

fn main() {
    let mut db = wwcdb::wwc_db::open("create_table.db");
    let mut key = Data_item_info::new();
    let mut value = Data_item_info::new();
    key.add((DATATYPE::U32(0),String::from("student_id")));
    let name = Db_str::new_container(12);
    value.add((DATATYPE::STR(name),String::from("name")));
    value.add((DATATYPE::F32(0.0),String::from("score")));
    db.create_table(key,value,String::from("student_table"));
    println!("{:?}",db.table_btrees);
}