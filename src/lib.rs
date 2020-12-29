use std::fs::{File, OpenOptions};
use crate::page::{Page, PageType, Pos, PageError, PAGE_SIZE, PTR_SIZE};
pub use crate::byte::*;
use anyhow::Result;
use std::fmt::Debug;
use std::rc::Rc;
use std::cell::{RefCell, RefMut};
use crate::pager_manager::Pager_manager;
use crate::db_str::Db_str;
use crate::data_item::{Data_item, Data_item_info};
use std::ops::Deref;
use std::hash::Hash;
use std::collections::HashMap;
use std::borrow::{BorrowMut, Borrow};

mod page;
mod byte;
pub mod db_str;
pub mod data_item;
mod pager_manager;
pub mod wwcdb;

#[derive(Debug)]
pub struct Page_info{
    keys_pos: usize,
    keys_internal_pos: usize,
    values_pos: usize,
    pub ptrs_pos: usize,
    max_item_count: usize,
    max_index_count: usize,
    _key: Data_item_info,
    _value: Data_item_info,
    key_size: usize,
    value_size: usize
}
impl Default for Page_info {
    fn default() -> Self {
        Page_info{
            keys_pos: 0,
            keys_internal_pos: 0,
            values_pos: 0,
            ptrs_pos: 0,
            max_item_count: 0,
            max_index_count: 0,
            _key: Data_item_info::new(),
            _value: Data_item_info::new(),
            key_size: 0,
            value_size: 0
        }
    }
}
#[derive(Debug)]
pub struct BTree
{
    // fd: Rc<RefCell<File>>,
    pager_manager: Rc<RefCell<Pager_manager>>,
    // keys_pos: usize,
    // keys_internal_pos: usize,
    // values_pos: usize,
    // ptrs_pos: usize,
    // max_item_count: usize,
    // max_index_count: usize,
    pager_info: Rc<RefCell<Page_info>>,
    // _key: Data_item,
    // _value: Data_item,
    // meta_page: Option<Page>,
    root_page: Rc<RefCell<Page>>,
    meta_page_index: u32,//元数据页其实不太用，不必拿page引用，记录页号就行
    index_btrees: Option<HashMap<String,Rc<RefCell<BTree>>>>
}

impl BTree
{
    pub fn new(pager_manager: Rc<RefCell<Pager_manager>>, key: Data_item_info, value: Data_item_info) -> Self {
        // let fd = OpenOptions::new()
        //     .create(true)
        //     .read(true)
        //     .write(true)
        //     .open(path).expect("could not open btree file");
        // let key = data_item::Data_item::new();
        // let value = data_item::Data_item::new();
        let temp = pager_manager.deref().borrow_mut().new_page(PageType::META).unwrap();
        let mut meta_pager = temp.deref().borrow_mut();
        let root_page = pager_manager.deref().borrow_mut().new_page(PageType::LEAF).unwrap();
        meta_pager.set_root_index(root_page.deref().borrow().index);
        meta_pager.set_attributes(&key, &value);
        let mut btree = BTree{
            pager_manager,
            pager_info: Rc::new(RefCell::new(Page_info::default())),
            // _key: key,
            // _value: value,
            root_page,
            meta_page_index: meta_pager.borrow().index,
            index_btrees: None
        };
        btree.pager_info.deref().borrow_mut()._key = key;
        btree.pager_info.deref().borrow_mut()._value = value;
        btree.init_btree();
        btree
    }

    pub fn load_btree(meta_page: Rc<RefCell<Page>>, pager_manager: Rc<RefCell<Pager_manager>>, pager_manager_ref: &mut RefMut<Pager_manager>) -> BTree {
        let meta = meta_page.deref().borrow_mut();
        let root_page = pager_manager_ref.get_page(meta.root_index()).unwrap();
        // println!("meta.root_index()={:?}",meta.root_index());
        println!("meta={:?}",meta);
        // println!("root_page={:?}",root_page);
        let (key, value, index) = meta.get_attributes_index();
        let page_info =  Rc::new(RefCell::new(Page_info::default()));
        page_info.deref().borrow_mut()._key = key;
        page_info.deref().borrow_mut()._value = value;
        let mut btree = BTree{
            pager_manager,
            // _key: key,
            // _value: value,
            root_page,
            meta_page_index: meta.index,
            index_btrees: index,
            pager_info: page_info
        };
        btree.init_btree();
        btree
    }

    pub fn add_index_tree(&self, key: Data_item_info){
        // BTree::new(self.pager_manager.clone(), key, self._value.clone());
        //
        //
    }

    pub fn get_info(&self) -> Rc<RefCell<Page_info>> {
        self.pager_info.clone()
    }

    // fn sync(&mut self) -> Result<()>{
    //     if let Some(p) = self.meta_page.as_mut() {
    //         p.sync()?;
    //     }
    //     if let Some(p) = self.root_page.as_mut() {
    //         p.sync()?;
    //     }
    //     Ok(())
    // }

    // fn init_as_empty(&mut self) {
    //     println!("init empty btree");
    //     let mut meta_page = Page::new(self.fd.clone(), 0, PageType::META).unwrap();
    //     meta_page.set_total_page(2);
    //     meta_page.set_root_index(1);
    //     let mut root_page = Page::new(self.fd.clone(), 1, PageType::LEAF).unwrap();
    //     root_page.set_item_count(0).unwrap();
    //
    //     self.meta_page = Some(meta_page);
    //     self.root_page = Some(root_page);
    //     self.sync().unwrap();
    // }
    //
    // fn init_load(&mut self) {
    //     let meta_page = Page::load(self.fd.clone(), 0).unwrap();
    //     assert_eq!(meta_page.page_type, PageType::META);
    //
    //     let root_page = Page::load(self.fd.clone(), meta_page.root_index()).unwrap();
    //     println!("root page index: {}; total pages:{}; root page keys: {};", meta_page.root_index(), meta_page.total_pages(), root_page.item_count());
    //     self.meta_page = Some(meta_page);
    //     self.root_page = Some(root_page);
    // }

    pub fn set(&mut self, key: &data_item::Data_item, value: &data_item::Data_item) -> Result<()> {
        let mut pages = Vec::new();
        loop {
            let mut p = self.root_page.deref().borrow_mut();
            match p.page_type {
                PageType::INTERNAL => {
                    match p.find(key) {
                        Some((i, pos)) => {
                            let ptr_index = match pos {
                                Pos::Left => {
                                    i
                                }
                                _ => {
                                   i + 1
                                }
                            };
                            let child_page_index = p.ptr_at(ptr_index).unwrap();
                            let child = self.get_btree_page(child_page_index);
                            pages.push(child);
                            let p = pages.get(pages.len() - 1).unwrap().deref().borrow_mut();
                            // let  c = pages.get(pages.len() - 1).as_deref().clone().borrow_mut();
                            // p = pages.get(pages.len() - 1).unwrap().deref().clone().borrow_mut();
                        }
                        None => {
                            panic!("impossible for an empty internal page")
                        }
                    }
                }
                PageType::LEAF => {
                    match p.insert(key, value) {
                        Ok(_) => {
                            // inserted, done!
                            return Ok(());
                        },
                        Err(err) => {
                            match err.downcast_ref::<PageError>() {
                                Some(PageError::Full) => {
                                    // eh..., the page is full, we need to split it
                                    break;
                                }
                                _ => {
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
                _ => {
                    panic!("impossible a meta page")
                }
            }
        }
        // page is full, split it!
        // println!("page is full");
        let mut kp = None;
        for mut p in pages.iter().rev().map(|x| x.deref().borrow_mut()) {
            match p.page_type {
                PageType::LEAF => {
                    // leaf page must be full in this case
                    kp = Some(self.split_leaf_page(&mut p, key, value)?);
                }
                PageType::INTERNAL => {
                    let (k, ptr) = kp.unwrap();
                    if p.is_full() {
                        kp = Some(self.split_internal_page(&mut p, &k, ptr)?);
                    } else {
                        p.insert_ptr(&k, ptr)?;
                        return Ok(());
                    }
                }
                _ => {
                    panic!("impossible a meta page")
                }
            }
        }

        // so root page must be changed
        match kp {
            Some((k, ptr)) => {
                let is_root_full;
                {
                    let root_page = self.root_page.deref().borrow_mut();
                    assert_eq!(root_page.page_type, PageType::INTERNAL);
                    is_root_full = root_page.is_full();
                }

                if !is_root_full {
                    let mut root_page = self.root_page.deref().borrow_mut();
                    root_page.insert_ptr(&k, ptr)?;
                } else {
                    let (k2, ptr2) = self.split_internal_page(&mut self.root_page.deref().borrow_mut(), &k, ptr)?;
                    let mut new_root_page = self.pager_manager.deref().borrow_mut().new_page(PageType::INTERNAL)?;
                    new_root_page.deref().borrow_mut().set_item_count(1)?;
                    new_root_page.deref().borrow_mut().set_ptr_at(0, self.root_page.deref().borrow_mut().index)?;
                    new_root_page.deref().borrow_mut().set_key_at(0, &k2)?;
                    new_root_page.deref().borrow_mut().set_ptr_at(1, ptr2)?;

                    {
                        let mut root_page = self.root_page.deref().borrow_mut();
                        root_page.set_root_index(new_root_page.deref().borrow_mut().index);
                    }
                    self.root_page = new_root_page;
                }
            }
            None => {
                // root page is full, do split !!!
                let new_root_page_res;
                {
                    let mut root_page = self.root_page.deref().borrow_mut();
                    assert!(root_page.is_full() && root_page.page_type == PageType::LEAF);
                    let (k, ptr) = self.split_leaf_page(&mut root_page, key, value)?;
                    let mut new_root_page = self.pager_manager.deref().borrow_mut().new_page(PageType::INTERNAL)?;
                    new_root_page.deref().borrow_mut().set_item_count(1)?;
                    new_root_page.deref().borrow_mut().set_ptr_at(0, root_page.index)?;
                    new_root_page.deref().borrow_mut().set_key_at(0, &k)?;
                    new_root_page.deref().borrow_mut().set_ptr_at(1, ptr)?;

                    let temp = self.get_btree_page(self.meta_page_index);
                    let mut meta_page = temp.deref().borrow_mut();
                    meta_page.set_root_index(new_root_page.deref().borrow_mut().index);
                    new_root_page_res = new_root_page;
                }
                self.root_page =new_root_page_res;
            }
        }
        // self.sync()?;
        Ok(())
    }

    pub fn get(&mut self, key: &Data_item) -> Option<Data_item> {
        let mut pages = Vec::new();
        // let mut p = self.root_page.as_ref().unwrap().deref().borrow_mut();
        loop {
            let mut p = self.root_page.deref().borrow_mut();
            // println!("{:?} {}", p.page_type, p.item_count());
            match p.find(key) {
                Some((i, pos)) => {
                    match p.page_type {
                        PageType::LEAF => {
                            // println!("i: {}, pos: {:?}", i, pos);
                            // println!("{:?}", p);
                            return if pos == Pos::Current {
                                p.value_at(i)
                            } else {
                                None
                            }
                        }
                        PageType::INTERNAL => {
                            match pos {
                                Pos::Left => {
                                    let child = self.get_btree_page(p.ptr_at(i).unwrap());
                                    pages.push(child);
                                    p = pages.get(pages.len()-1).unwrap().deref().borrow_mut();
                                },
                                _ => {
                                    let child = self.get_btree_page(p.ptr_at(i+1).unwrap());
                                    pages.push(child);
                                    p = pages.get(pages.len()-1).unwrap().deref().borrow_mut();
                                }
                            }
                        }
                        _ => {
                            // impossible
                            return None
                        }
                    }
                },
                None => {
                    return None;
                }
            }
        }
    }

    // fn new_page(&mut self, pt: PageType) -> Result<Page> {
    //     let meta_page = self.meta_page.as_mut().unwrap();
    //     let max_index = meta_page.total_pages();
    //     meta_page.set_total_page(max_index + 1);
    //     Ok(Page::<K, V>::new(self.fd.clone(), max_index, pt)?)
    // }

    fn split_leaf_page(& self, p: &mut RefMut<Page>, key: &Data_item, value: &Data_item) -> Result<(Data_item, u32)> {
        assert_eq!(p.page_type, PageType::LEAF);
        let temp = self.pager_manager.deref().borrow_mut().new_page(PageType::LEAF)?;
        let mut new_page = temp.deref().borrow_mut();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut inserted = false;
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                values.push(value.clone());
                inserted = true;
            }
            keys.push(k);
            values.push(p.value_at(i).unwrap())
        }
        if !inserted {
            keys.push(key.clone());
            values.push(value.clone());
            // inserted = true;
        }
        let cut_i =  (keys.len() + 1) / 2;
        p.set_item_count(cut_i)?;
        new_page.set_item_count(keys.len() - cut_i)?;

        for i in 0..cut_i {
            p.set_key_at(i, &keys[i])?;
            p.set_value_at(i, &values[i])?;
        }

        for i in cut_i..keys.len() {
            new_page.set_key_at(i - cut_i, &keys[i])?;
            new_page.set_value_at(i - cut_i, &values[i])?;
        }
        Ok((keys[cut_i].clone(), new_page.index))
    }

    fn split_internal_page(& self, p: &mut RefMut<Page>, key: &Data_item, ptr: u32) -> Result<(Data_item, u32)> {
        assert_eq!(p.page_type, PageType::INTERNAL);
        let temp = self.pager_manager.deref().borrow_mut().new_page(PageType::INTERNAL)?;
        let mut new_page = temp.deref().borrow_mut();
        let mut keys = Vec::new();
        let mut ptrs = Vec::new();
        let mut inserted = false;
        ptrs.push(p.ptr_at(0).unwrap());
        for i in 0..p.item_count() {
            let k = p.key_at(i).unwrap();
            if !inserted && k > *key {
                keys.push(key.clone());
                ptrs.push(ptr);
                inserted = true;
            }
            keys.push(k);
            ptrs.push(p.ptr_at(i + 1).unwrap());
        }

        if !inserted {
            keys.push(key.clone());
            ptrs.push(ptr);
            // inserted = true;
        }

        let up_i =  (keys.len() - 1) / 2;
        p.set_item_count(up_i)?;
        new_page.set_item_count(keys.len() - up_i - 1)?;

        for i in 0..up_i {
            p.set_key_at(i, &keys[i])?;
            p.set_ptr_at(i + 1, ptrs[i + 1])?;
        }

        new_page.set_ptr_at(0, ptrs[up_i + 1])?;
        for i in (up_i + 1)..keys.len() {
            new_page.set_key_at(i - up_i - 1, &keys[i])?;
            new_page.set_ptr_at(i - up_i, ptrs[i + 1])?;
        }
        Ok((keys[up_i].clone(), new_page.index))
    }

    fn init_btree(&mut self) {
        let mut info = self.pager_info.deref().borrow_mut();
        info.key_size = info._key.get_data_item().bin_size();
        info.value_size = info._value.get_data_item().bin_size();
        info.max_index_count = (PAGE_SIZE - 8 - PTR_SIZE) / (info.key_size + PTR_SIZE);
        info.max_item_count = (PAGE_SIZE - 12) / (info.key_size + info.value_size);
        info.keys_pos = 12;
        info.keys_internal_pos = 8;
        info.ptrs_pos = info.keys_internal_pos + info.max_index_count * info.key_size;
        info.values_pos = info.keys_pos + info.max_item_count * info.key_size;
        // println!("{:?}",self.pager_info);
        self.root_page.deref().borrow_mut().page_info = Some(self.pager_info.clone());
    }

    fn get_btree_page(& self, index: u32) -> Rc<RefCell<Page>> {
        let res = self.pager_manager.deref().borrow_mut().get_page(index).unwrap();
        {
            let mut temp = res.deref().borrow_mut();
            match temp.page_type {
                PageType::INTERNAL | PageType::LEAF => {
                    temp.page_info = Some(self.pager_info.clone())
                }
                _ => {}
            }
        }
        res
    }
}
// #[cfg(test)]
// mod test{
//     use crate::{wwcdb, DATATYPE};
//     use std::ops::Deref;
//     use crate::db_str::Db_str;
//
//     #[test]
//     fn insert_record() {
//         let mut db = wwcdb::wwc_db::open("create_table.db");
//         let res = db.get_data_item_info("student_table").unwrap();
//         assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
//         let key = res.deref().borrow_mut()._key.get_data_item();
//         assert!(res.deref().borrow_mut()._value.set(DATATYPE::STR(Db_str::new("xiaoming",12)),"name"));
//         println!("44:{:?}",res.deref().borrow_mut()._value);
//         assert!(res.deref().borrow_mut()._value.set(DATATYPE::F32(78.4),"score"));
//         let value = res.deref().borrow_mut()._value.get_data_item();
//         db.insert_record(key,value,"student_table");
//     }
//
//     #[test]
//     fn query_record_by_promary_key() {
//         let mut db = wwcdb::wwc_db::open("create_table.db");
//         let res = db.get_data_item_info("student_table").unwrap();
//         assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
//         let key = res.deref().borrow_mut()._key.get_data_item();
//         let value = res.deref().borrow_mut()._value.get_data_item();
//         db.query_by_primary_key(&key,"student_table");
//     }
//
//     #[test]
//     fn update_record() {
//         //更新小明的成绩为99.9分
//         let mut db = wwcdb::wwc_db::open("create_table.db");
//         let res = db.get_data_item_info("student_table").unwrap();
//         assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
//         let key = res.deref().borrow_mut()._key.get_data_item();
//         assert!(res.deref().borrow_mut()._value.clear_value().set(DATATYPE::F32(99.9),"score"));
//         //在不知道小明名字的情况下，根据键值,只改了分数
//         let update_value = res.deref().borrow_mut()._value.get_data_item();
//         db.update_record(key,update_value,"student_table");
//     }
//     #[test]
//     fn delete_record() {
//         //更新小明的成绩为99.9分
//         let mut db = wwcdb::wwc_db::open("create_table.db");
//         let res = db.get_data_item_info("student_table").unwrap();
//         assert!(res.deref().borrow_mut()._key.set(DATATYPE::U32(56),"student_id"));
//         let key = res.deref().borrow_mut()._key.get_data_item();
//         // assert!(res.deref().borrow_mut()._value.clear_value().set(DATATYPE::F32(99.9),"score"));
//         //在不知道小明名字的情况下，根据键值,只改了分数
//         let update_value = res.deref().borrow_mut()._value.get_data_item();
//         db.delete_record(key,update_value,"student_table");
//     }
// }
