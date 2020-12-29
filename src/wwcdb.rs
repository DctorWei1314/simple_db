use std::cell::RefCell;
use std::rc::Rc;
use crate::pager_manager::Pager_manager;
use crate::{BTree, Attribute, DATATYPE, toDATATYPE, Page_info};
use crate::data_item::{Data_item, Data_item_info};
use anyhow::{Result, anyhow};
use std::borrow::{BorrowMut, Borrow};
use std::collections::HashMap;
use std::ops::Deref;
use crate::page::{Page, PageType, PAGE_SIZE, TABLE_NAME_SIZE, DB_META_ITEM_START, DB_META_ITEM_SIZE, MAX_META_DB_ITEM};
use crate::db_str::Db_str;
use std::io::{SeekFrom, Seek, Read};

pub struct wwc_db{
    pager_manager: Rc<RefCell<Pager_manager>>,
    pub table_btrees: HashMap<String,Rc<RefCell<BTree>>>
}

impl wwc_db {

    pub fn open(path: &str) -> wwc_db {
        let pager_manager = Rc::new(RefCell::new(Pager_manager::new(path)));
        let table_btrees =  wwc_db::get_db_tables(0,pager_manager.clone());
        wwc_db{
            pager_manager,
            table_btrees
        }
    }

    pub fn get_db_tables(index: u32, pager_manager: Rc<RefCell<Pager_manager>>) -> HashMap<String,Rc<RefCell<BTree>>> {
        let mut res = HashMap::new();
        let mut pager_manager_ref = pager_manager.deref().borrow_mut();
        let temp = pager_manager_ref.get_page(index).unwrap();
        let db_meta_page = temp.deref().borrow();
        assert_eq!(db_meta_page.page_type, PageType::DB_META);
        if db_meta_page.item_count() <= 0 {
            return res;
        }
        let mut i = 0;
        while i < db_meta_page.item_count() {
            let mut name = DATATYPE::STR(Db_str::new_container(TABLE_NAME_SIZE));
            toDATATYPE(name.decode(&db_meta_page.buf[DB_META_ITEM_START+DB_META_ITEM_SIZE*i..]),& mut name);
            let name_value = match name { DATATYPE::STR(value) => value , _ => panic!("can't happen") };
            let mut ptr = DATATYPE::U32(0);
            toDATATYPE(ptr.decode(&db_meta_page.buf[DB_META_ITEM_START + DB_META_ITEM_SIZE*i +TABLE_NAME_SIZE..]),&mut ptr);
            let ptr_value = match ptr { DATATYPE::U32(value) => value , _ => panic!("can't happen") };
            println!("{:?} {:?} {:?}",name_value,ptr_value,db_meta_page);
            let btree_meta_page = pager_manager_ref.get_page(ptr_value).unwrap();
            let btree = BTree::load_btree(btree_meta_page, pager_manager.clone(), &mut pager_manager_ref);
            res.insert(name_value.str,Rc::new(RefCell::new(btree)));
            println!("ddd");
            i = i + 1;
        };
        let mut page_index = db_meta_page.next_page();
        if page_index != 0 {
            // let page = pager_manager.deref().borrow_mut().get_page(page_index).unwrap();
            // let page_ref = page.deref().borrow();
            res.extend(wwc_db::get_db_tables(page_index, pager_manager.clone()));
        }
        res
    }

    pub fn add_db_table(&mut self, table: (&String,u32)) {
        let pager_manager = & mut self.pager_manager.deref().borrow_mut();
        let mut page_index = pager_manager.db_meta_page.deref().borrow_mut().next_page();
        if page_index == 0 {
            let name = Db_str::new(table.0,TABLE_NAME_SIZE);
            let num = pager_manager.db_meta_page.deref().borrow_mut().item_count();
            if num >= MAX_META_DB_ITEM {
                let new_page = pager_manager.new_page(PageType::DB_META).unwrap();
                new_page.deref().borrow_mut().set_next_page(new_page.deref().borrow().index);
                name.encode(&mut new_page.deref().borrow_mut().buf[DB_META_ITEM_START..]);
                table.1.encode(&mut new_page.deref().borrow_mut().buf[DB_META_ITEM_START + TABLE_NAME_SIZE..]);
                new_page.deref().borrow_mut().set_item_count(1);
            } else {
                name.encode(&mut pager_manager.db_meta_page.deref().borrow_mut().buf[DB_META_ITEM_START + DB_META_ITEM_SIZE *num..]);
                table.1.encode(&mut pager_manager.db_meta_page.deref().borrow_mut().buf[DB_META_ITEM_START + DB_META_ITEM_SIZE*num + TABLE_NAME_SIZE..]);
                pager_manager.db_meta_page.deref().borrow_mut().set_item_count(num + 1);
            }
        }
        else {
            // let mut temp= Vec::new();
            // temp.push(pager_manager.get_page(page_index).unwrap());
            // page = temp[temp.len()-1].deref().borrow_mut();
            // page_index = page.next_page();
            // while page_index != 0 {
            //     temp.push(pager_manager.get_page(page_index).unwrap());
            //     page = temp[temp.len()-1].deref().borrow_mut();
            //     page_index = page.next_page();
            // }
            let temp = self.get_new_db_meta_page(page_index);
            let mut page = temp.deref().borrow_mut();
            assert_eq!(page.page_type, PageType::DB_META);
            let name = Db_str::new(table.0,TABLE_NAME_SIZE);
            let num = page.item_count();
            if num >= MAX_META_DB_ITEM {
                let new_page = pager_manager.new_page(PageType::DB_META).unwrap();
                page.set_next_page(new_page.deref().borrow().index);
                name.encode(&mut page.buf[DB_META_ITEM_START..]);
                table.1.encode(&mut page.buf[DB_META_ITEM_START + TABLE_NAME_SIZE..]);
                page.set_item_count(1);
            } else {
                name.encode(&mut page.buf[DB_META_ITEM_START + DB_META_ITEM_SIZE *num..]);
                table.1.encode(&mut page.buf[DB_META_ITEM_START + DB_META_ITEM_SIZE*num + TABLE_NAME_SIZE..]);
                page.set_item_count(num + 1);
            }
        }
    }

    fn get_new_db_meta_page(&self,page_index: u32) -> Rc<RefCell<Page>> {
        let mut pager_manager = self.pager_manager.deref().borrow_mut();
        let index = page_index;
        let mut res = Rc::new(RefCell::new(Default::default()));
        loop {
            res = pager_manager.get_page(index).unwrap();
            if res.deref().borrow().next_page() == 0 {
                break;
            }
        }
        res
    }

    pub fn create_table(&mut self, key: Data_item_info, value: Data_item_info, name:  String) -> Result<()> {
        let btree = BTree::new(self.pager_manager.clone(), key, value);
        self.add_db_table((&name, btree.meta_page_index));
        self.table_btrees.borrow_mut().insert(name, Rc::new(RefCell::new(btree)));
        Ok(())
    }

    pub fn insert_record(&mut self, key: Data_item, value: Data_item, name:  &str) -> Result<(),()> {
        let table_option = self.table_btrees.get(name);
        if table_option.is_none() {return Err(())}
        let mut table = table_option.unwrap().deref().borrow_mut();
        table.set(&key,&value);
        Ok(())
    }

    pub fn update_record(&mut self, key: Data_item, value: Data_item, name:  &str) -> Result<(),()> {
        let table_option = self.table_btrees.get(name);
        if table_option.is_none() {return Err(())}
        let mut table = table_option.unwrap().deref().borrow_mut();
        table.set(&key,&value);
        Ok(())
    }

    pub fn query_by_primary_key(& self, key: &Data_item, name:  &str) -> Result<(Data_item,Data_item),()> {
        let table_option = self.table_btrees.get(name);
        if table_option.is_none() {return Err(())}
        let mut table = table_option.unwrap().deref().borrow_mut();
        // table.set(&key,&value);
        let value = table.get(key);
        let y = (key.clone(), value.unwrap());
        println!("(key.clone(), value.unwrap()):{:?}",y);
        Ok(y)
    }
    pub fn delete_record(&mut self, key: Data_item, value: Data_item, name:  &str) {
    }

    pub fn query_all_record(& self, name:  &str) -> Result<Vec<(Data_item,Data_item)>,()> {
    //
        Err(())
    }

    pub fn query_by_index_key(& self, key: Data_item, name:  &str) -> Result<(Data_item,Data_item),()> {
    //
        Err(())
    }

    pub fn get_data_item_info(&mut self,name:  &str) -> Result<(Rc<RefCell<Page_info>>),()> {
        let table_option = self.table_btrees.get(name);
        if table_option.is_none() {return Err(())}
        let mut table = table_option.unwrap().deref().borrow_mut();
        Ok(table.get_info())
    }

    pub fn create_index(&mut self, name: String, key: String) -> Result<()> {
        Ok(())
    }
}
#[cfg(test)]
mod test{
    use crate::{wwcdb, DATATYPE};
    use crate::data_item::{Data_item_info, Data_item};
    use crate::db_str::Db_str;
    use std::ops::Deref;

    #[test]
    fn test_create_table() {
        let mut db = wwcdb::wwc_db::open("create_table.db");
        let mut key = Data_item_info::new();
        let mut value = Data_item_info::new();
        key.add((DATATYPE::U32(0),String::from("student_id")));
        //主键
        let name = Db_str::new_container(12);
        value.add((DATATYPE::STR(name),String::from("name")));
        value.add((DATATYPE::F32(0.0),String::from("score")));
        //纪录体
        db.create_table(key,value,String::from("student_table"));
        println!("{:?}",db.table_btrees);
    }

    #[test]
    fn open_exist_file() {
        let mut db = wwcdb::wwc_db::open("create_table.db");
        //刚刚创建的文件
        // let mut key = Data_item_info::new();
        // let mut value = Data_item_info::new();
        // key.add((DATATYPE::U32(0),String::from("student_id")));
        // //主键
        // let name = Db_str::new_container(12);
        // value.add((DATATYPE::STR(name),String::from("name")));
        // value.add((DATATYPE::F32(0.0),String::from("score")));
        // //纪录体
        // db.create_table(key,value,String::from("student_table"));
        println!("{:?}",db.table_btrees);
    }

    #[test]
    fn insert_record() {
        let mut db = wwcdb::wwc_db::open("create_table.db");
        let res = db.get_data_item_info("student_table").unwrap();
        assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
        let key = res.deref().borrow_mut()._key.get_data_item();
        assert!(res.deref().borrow_mut()._value.set(DATATYPE::STR(Db_str::new("xiaoming",12)),"name"));
        println!("44:{:?}",res.deref().borrow_mut()._value);
        assert!(res.deref().borrow_mut()._value.set(DATATYPE::F32(78.4),"score"));
        let value = res.deref().borrow_mut()._value.get_data_item();
        db.insert_record(key,value,"student_table");
    }

    #[test]
    fn query_record_by_promary_key() {
        let mut db = wwcdb::wwc_db::open("create_table.db");
        let res = db.get_data_item_info("student_table").unwrap();
        assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
        let key = res.deref().borrow_mut()._key.get_data_item();
        let value = res.deref().borrow_mut()._value.get_data_item();
        db.query_by_primary_key(&key,"student_table");
    }

    #[test]
    fn update_record() {
        //更新小明的成绩为99.9分
        let mut db = wwcdb::wwc_db::open("create_table.db");
        let res = db.get_data_item_info("student_table").unwrap();
        assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
        let key = res.deref().borrow_mut()._key.get_data_item();
        assert!(res.deref().borrow_mut()._value.clear_value().set(DATATYPE::F32(99.9),"score"));
        //在不知道小明名字的情况下，根据键值,只改了分数
        let update_value = res.deref().borrow_mut()._value.get_data_item();
        db.update_record(key,update_value,"student_table");
    }
}