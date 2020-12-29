use std::fs::File;
use anyhow::{Result, anyhow};
use std::io::{Read, Seek, SeekFrom, Write};
use std::borrow::{BorrowMut, Borrow};
use std::marker::PhantomData;
use thiserror::Error;
use std::fmt::{Debug, Formatter};
use std::cell::RefCell;
use std::rc::Rc;
use crate::{Attribute, BTree, Page_info, toDATATYPE, DATATYPE, get_u32, get_db_str, get_type, set_type};
use crate::data_item::{Data_item, Data_item_info};
use std::collections::HashMap;
use std::ops::Deref;
use chrono::format::Numeric::Day;
use crate::db_str::Db_str;

pub const PAGE_SIZE: usize = 4096;
pub const PTR_SIZE: usize = 4;
pub const ATTRIBUTE_NAME_SIZE: usize = 16;
pub const TABLE_NAME_SIZE: usize = 16;
pub const DB_META_ITEM_START: usize = 16;
pub const MAX_META_DB_ITEM: usize = (PAGE_SIZE - 16) / (TABLE_NAME_SIZE + PTR_SIZE);
pub const MAX_META_ITEM: usize = (PAGE_SIZE - 20) / (TABLE_NAME_SIZE + PTR_SIZE);
pub const DB_META_ITEM_SIZE:usize = TABLE_NAME_SIZE + PTR_SIZE;
pub const META_ITEM_SIZE:usize = ATTRIBUTE_NAME_SIZE + PTR_SIZE;

#[derive(Error, Debug)]
pub enum PageError {
    #[error("page is full, need split")]
    Full
}

#[derive(Debug)]
pub struct Page
{
    pub index: u32,
    pub(crate) buf: [u8; PAGE_SIZE],
    pub page_type: PageType,
    // keys_pos: usize,
    // values_pos: usize,
    // ptrs_pos: usize,
    // max_item_count: usize,
    dirty: bool,
    pub(crate) fd: Option<Rc<RefCell<File>>>,
    pub page_info: Option<Rc<RefCell<Page_info>>>
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum PageType {
    DB_META,
    META,
    INTERNAL,
    LEAF,
}

#[derive(Debug, PartialOrd, PartialEq)]
pub enum Pos {
    Current,
    Left,
    Right
}

impl Default for Page {
    fn default() -> Self {
        Page {
            index: 0,
            buf: [0; PAGE_SIZE],
            page_type: PageType::LEAF,
            // keys_pos: 0,
            // values_pos: 0,
            // ptrs_pos: 0,
            // max_item_count: 0,
            dirty: false,
            fd: None,
            page_info: None
        }
    }
}

impl Page
{
    pub fn new(fd :Rc<RefCell<File>>, index: u32, pt: PageType) -> Result<Self> {
        let mut page = Self::default();
        page.page_type = pt;
        page.index = index;
        page.fd = Some(fd);
        page.dirty = true;
        match page.page_type{
            PageType::META => {
                page.buf[0] = 0x01;
                page.set_item_count(0);
                page.set_attribute_num(0);
                page.set_root_index(0);
                page.set_next_page(0);
            }
            PageType::INTERNAL => {
                page.buf[0] = 0x02;
                page.set_item_count(0).unwrap();
            }
            PageType::LEAF => {
                page.buf[0] = 0;
                page.set_item_count(0).unwrap();
            }
            PageType::DB_META => {
                page.buf[0] = 0x03;
                page.set_item_count(0);
                page.set_total_page(1);
                page.set_next_page(0);
            }
        }
        // page.init_layout();
        Ok(page)
    }

    // fn init_layout(&mut self) {
    //     match self.page_type{
    //         PageType::META => {
    //         }
    //         PageType::INTERNAL => {
    //             self.max_item_count = (PAGE_SIZE - 8 - PTR_SIZE) / (K.bin_size() + PTR_SIZE);
    //             self.keys_pos = 8;
    //             self.ptrs_pos = self.keys_pos + self.max_item_count * K.bin_size()
    //         }
    //         PageType::LEAF => {
    //             self.max_item_count = (PAGE_SIZE - 8) / (K.bin_size() + V.bin_size());
    //             self.keys_pos = 8;
    //             self.values_pos = self.keys_pos + self.max_item_count * K.bin_size();
    //         }
    //     };
    //     // at least we should have two items in one page
    //     assert!(self.page_type == PageType::META || self.max_item_count >= 2)
    // }

    // pub fn load(index: u32) -> Result<Self> {
    //     let mut page = Self::default();
    //
    //     {
    //         let mut _fd = fd.as_ref().borrow_mut();
    //         page.index = index;
    //         _fd.seek(SeekFrom::Start((index as usize * PAGE_SIZE) as u64))?;
    //         _fd.read_exact(page.buf.borrow_mut())?;
    //     }
    //
    //     page.page_type = page.get_page_type();
    //     page.fd = Some(fd);
    //     page.init_layout();
    //     Ok(page)
    // }

    fn mark_dirty(&mut self) {
        self.dirty = true
    }

    pub(crate) fn get_page_type(&self) -> PageType {
        let u = self.buf[0];
        if u == 1 {
            PageType::META
        } else {
            if u == 2 {
                PageType::INTERNAL
            } else {
                if u == 3 {
                    PageType::DB_META
                } else {
                    PageType::LEAF
                }
            }
        }
    }

    pub fn root_index(&self) -> u32 {
        let mut res = DATATYPE::U32(0);
        match self.page_type {
            PageType::META => {
                toDATATYPE(res.decode(&self.buf[16..]), &mut res);
                match res {
                    DATATYPE::U32(res) => res,
                    _ => panic!("can't happen")
                }
            },
            _ => panic!("not a meta page")
        }
    }

    pub fn total_pages(&self) -> u32 {
        let mut res = DATATYPE::U32(0);
        match self.page_type {
            PageType::DB_META => {
                toDATATYPE(res.decode(&self.buf[4..]), &mut res);
                match res {
                    DATATYPE::U32(res) => res,
                    _ => panic!("can't happen")
                }
            }
            _ => panic!("not a meta page")
        }
    }

    pub fn set_root_index(&mut self, root_index: u32) {
        match self.page_type {
            PageType::META => {
                root_index.encode(&mut self.buf[16..]);
                self.mark_dirty();
            }
            _ => panic!("not a meta page")
        }
    }
    // pub fn set_item_count(&mut self, item_count: u32) {
    //     match self.page_type {
    //
    //         _ => panic!("not a meta page")
    //     }
    // }

    pub fn set_total_page(&mut self, total_page: u32) {
        match self.page_type {
            PageType::DB_META => {
                total_page.encode(&mut self.buf[4..]);
                self.mark_dirty();
            },
            _ => panic!("not a meta page")
        }
    }

    pub fn set_attribute_num(&mut self, attribute_num: u32) {
        match self.page_type {
            PageType::META => {
                attribute_num.encode(&mut self.buf[4..]);
                self.mark_dirty();
            },
            _ => panic!("not a meta page")
        }
    }

    pub fn attribute_num(& self) -> u32{
        let mut res = DATATYPE::U32(0);
        match self.page_type {
            PageType::META => {
                toDATATYPE(res.decode(&self.buf[8..]), &mut res);
                match res {
                    DATATYPE::U32(res) => res,
                    _ => panic!("can't happen")
                }
            },
            _ => panic!("need a meta page")
        }
    }

    pub fn set_next_page(&mut self, next_page: u32) {
        match self.page_type {
            PageType::META => {
                next_page.encode(&mut self.buf[12..]);
                self.mark_dirty();
            },
            PageType::DB_META => {
                next_page.encode(&mut self.buf[12..]);
                self.mark_dirty();
            },
            PageType::LEAF => {
                next_page.encode(&mut self.buf[8..]);
                self.mark_dirty();
            },
            _ => panic!("not a INTERNAL page")
        }
    }

    pub fn next_page(&self) -> u32 {
        let mut res = DATATYPE::U32(0);
        match &self.page_type {
            PageType::INTERNAL => panic!("not a INTERNAL page"),
            PageType::LEAF => {
                toDATATYPE(res.decode(&self.buf[8..]), &mut res);
                match res {
                    DATATYPE::U32(res) => res,
                    _ => panic!("can't happen")
                }
            }
            _ => {
                toDATATYPE(res.decode(&self.buf[12..]), &mut res);
                match res {
                    DATATYPE::U32(res) => res,
                    _ => panic!("can't happen")
                }
            }
        }
    }

    pub fn item_count(&self) -> usize {
        let mut res = DATATYPE::USIZE(0);
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF => {
                toDATATYPE(res.decode(&self.buf[4..]), &mut res);
                match res {
                    DATATYPE::USIZE(res) => res,
                    _ => panic!("can't happen")
                }
            }
            PageType::META | PageType::DB_META => {
                toDATATYPE(res.decode(&self.buf[8..]), &mut res);
                match res {
                    DATATYPE::USIZE(res) => res,
                    _ => panic!("can't happen")
                }
            }
        }
    }

    pub fn get_attributes_index(&self) -> (Data_item_info, Data_item_info, Option<HashMap<String,Rc<RefCell<BTree>>>>) {
        assert_eq!(self.page_type, PageType::META);
        let attribute_count_value = get_u32(&self.buf[4..]);
        let mut key = Data_item_info::new();
        let key_name_value = get_db_str(&self.buf[20..],ATTRIBUTE_NAME_SIZE);
        let data_type = get_type(&self.buf[20+ATTRIBUTE_NAME_SIZE..]);
        key.add((data_type,key_name_value.str));
        //第一个属性时key，后面的全是value
        let mut value = Data_item_info::new();
        // println!("attribute_count_value:{:?}",attribute_count_value);
        for i in 1..attribute_count_value {
            let value_name_value = get_db_str(&self.buf[20 + i as usize * META_ITEM_SIZE ..],ATTRIBUTE_NAME_SIZE);
            let data_type = get_type(&self.buf[20 + i as usize * META_ITEM_SIZE +  ATTRIBUTE_NAME_SIZE..]);
            value.add((data_type,value_name_value.str));
            // println!("value:{:?} {:?}",value, i);
        }
        // let index_count_value = get_u32(&self.buf[8..]);
            // let mut attribute_name = DATATYPE::STR(Db_str::new_container(ATTRIBUTE_NAME_SIZE));
        // toDATATYPE(attribute_name.decode(&self.buf[]),& mut attribute_name);
        // let name_value = match name { DATATYPE::STR(value) => value , _ => panic!("can't happen") };
        (key, value, Some(HashMap::new()))
    }

    pub fn set_attributes(&mut self, key: &Data_item_info, value: &Data_item_info) {
        assert_eq!(self.page_type, PageType::META);
        let attributes_len = value.attributes.len();
        self.set_attribute_num(attributes_len as u32 + 1);
        Db_str::new(&key.attributes[0].1,ATTRIBUTE_NAME_SIZE).encode(&mut self.buf[20..]);
        set_type(&mut self.buf[20 + ATTRIBUTE_NAME_SIZE..],&key.attributes[0].0);
        for i in 1..attributes_len + 1 {
            println!("***");
            Db_str::new(&value.attributes[i-1].1,ATTRIBUTE_NAME_SIZE).encode(&mut self.buf[20 + i as usize*META_ITEM_SIZE..]);
            set_type(&mut self.buf[20 + i as usize*META_ITEM_SIZE + ATTRIBUTE_NAME_SIZE..],&value.attributes[i-1].0);
        }
        println!("ddd:{:?}",self.buf);
    }

    pub fn add_index(&self, index: (String,u32)) {

    }

    pub fn attach_page_info(&mut self, page_info: Rc<RefCell<Page_info>>){
        match self.page_type{
            PageType::INTERNAL | PageType::LEAF => {
                self.page_info = Some(page_info);
            }
            _ => panic!("meta page don't need page_info attaching")
        }
    }

    pub fn is_meta_full(&self) -> bool {
        match self.page_type {
            PageType::DB_META => self.item_count() >= MAX_META_DB_ITEM,
            PageType::META => self.item_count() + self.attribute_num() as usize >= MAX_META_ITEM,
            _ => panic!("need a meta page")
        }
    }

    // fn is_full(& self, page: &Page) -> bool {
    //     match page.page_type {
    //         PageType::DBMETA => page.item_count() >= self.max_index_count,
    //         PageType::META => page.item_count() >= self.max_item_count,
    //         _ => panic!("need a meta page")
    //     }
    // }

    pub fn set_item_count(&mut self, item_count: usize) -> Result<()>{
        match self.page_type {
            PageType::INTERNAL | PageType::LEAF=> {
                if item_count == 0 {
                    item_count.encode(&mut self.buf[4..]);
                    self.mark_dirty();
                    return Ok(())
                }
                if item_count > match  &self.page_info {
                    None => panic!("not having a page info"),
                    Some(page_info) => {page_info.deref().borrow_mut().max_index_count}
                } {
                    Err(PageError::Full.into())
                } else {
                    item_count.encode(&mut self.buf[4..]);
                    self.mark_dirty();
                    Ok(())
                }
            },
            PageType::DB_META => {
                item_count.encode(&mut self.buf[8..]);
                self.mark_dirty();
                Ok(())
            }
            PageType::META => {
                item_count.encode(&mut self.buf[8..]);
                self.mark_dirty();
                Ok(())
            }
        }
    }

    pub fn key_at(&self, i: usize) -> Option<Data_item> {
        match self.page_type {
             PageType::LEAF=> {
                if i >= self.item_count() {
                    None
                } else {
                    match &self.page_info {
                        None => panic!("not have a page info"),
                        Some(page_info) => {
                            let page_info = page_info.deref().borrow_mut();
                            let res = page_info._key.get_data_item();
                            Some(res.from_vec(res.decode(&self.buf[(page_info.keys_pos + i * page_info.key_size)..])))
                        }
                    }
                }
            }
            PageType::INTERNAL=> {
                if i >= self.item_count() {
                    None
                } else {
                    match &self.page_info {
                        None => panic!("not have a page info"),
                        Some(page_info) => {
                            let page_info = page_info.deref().borrow_mut();
                            let res = page_info._key.get_data_item();
                            Some(res.from_vec(res.decode(&self.buf[(page_info.keys_internal_pos + i * page_info.key_size)..])))
                        }
                    }
                }
            }
            _ => panic!("not a internal / leaf page")
        }
    }

    pub fn value_at(&self, i: usize) -> Option<Data_item> {
        match self.page_type {
            PageType::LEAF => {
                if i >= self.item_count() {
                    None
                } else {
                    match &self.page_info{
                        None => panic!("not have a page info"),
                        Some(page_info) => {
                            let page_info = page_info.deref().borrow_mut();
                            let res = page_info._value.get_data_item();
                            Some(res.from_vec(res.decode(&self.buf[(page_info.values_pos + i * page_info.value_size)..])))
                        }
                    }
                }
            }
            _ => panic!("not a leaf page")
        }
    }

    pub fn ptr_at(&self, i: usize) -> Option<u32> {
        let mut res = DATATYPE::U32(0);
        match self.page_type {
            PageType::INTERNAL=> {
                if i >= self.item_count() + 1 {
                    None
                } else {
                    // let page_info = page_info.deref().borrow_mut();
                    // let res = page_info._value.get_data_item();
                    // Some(res.from_vec(res.decode(&self.buf[(page_info.values_pos + i * page_info.value_size)..])))
                    match &self.page_info {
                        None => panic!("not have a page info"),
                        Some(page_info) => {
                            toDATATYPE(res.decode(&self.buf[(page_info.deref().borrow().ptrs_pos+ i * PTR_SIZE)..]),& mut res);
                            match res {
                                DATATYPE::U32(res) => Some(res),
                                _ => panic!("can't happen")
                            }
                        }
                    }
                }
            }
            _ => panic!("not a internal page")
        }
    }

    pub fn set_key_at(&mut self, i: usize, key: &Data_item) -> Result<()> {
        match self.page_type {
            PageType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                match &self.page_info {
                    None => panic!("not have a page info"),
                    Some(page_info) => {
                        let page_info = page_info.deref().borrow_mut();
                        key.encode(&mut self.buf[(page_info.keys_pos + i * page_info.key_size)..])
                    }
                }
                self.mark_dirty();
                Ok(())
            },
            PageType::INTERNAL => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                match &self.page_info {
                    None => panic!("not have a page info"),
                    Some(page_info) => {
                        let page_info = page_info.deref().borrow_mut();
                        key.encode(&mut self.buf[(page_info.keys_internal_pos + i * page_info.key_size)..])
                    }
                }
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a internal / leaf page")
        }
    }

    pub fn set_value_at(&mut self, i: usize, value: &Data_item) -> Result<()> {
        match self.page_type {
            PageType::LEAF => {
                if i >= self.item_count() {
                    return Err(anyhow!("over size"))
                }
                match &self.page_info {
                    None => panic!("not have a page info"),
                    Some(page_info) => {
                        let page_info = page_info.deref().borrow_mut();
                        value.encode(&mut self.buf[(page_info.values_pos + i * page_info.value_size)..])
                    }
                }
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a leaf page")
        }
    }

    pub fn set_ptr_at(&mut self, i: usize, ptr: u32) -> Result<()> {
        match self.page_type {
            PageType::INTERNAL => {
                if i >= self.item_count() + 1 {
                    return Err(anyhow!("over size"))
                }
                match &self.page_info {
                    None => panic!("not have a page info"),
                    Some(page_info) => {
                        ptr.encode(&mut self.buf[(page_info.deref().borrow_mut().ptrs_pos + i * PTR_SIZE)..])
                    }
                }
                self.mark_dirty();
                Ok(())
            }
            _ => panic!("not a internal page")
        }
    }

    pub fn find(&self, k: &Data_item) -> Option<(usize, Pos)> {
        let item_count = self.item_count();
        if item_count == 0 {
            return None;
        }
        let mut min = 0;
        let mut max = item_count - 1;
        let mut mid;
        while min <= max {
            mid = (min + max) / 2;
            let mid_key = self.key_at(mid).unwrap();
            if mid_key == *k {
                return Some((mid, Pos::Current));
            } else if *k > mid_key {
                if mid == item_count - 1 || self.key_at(mid + 1).unwrap() > *k {
                    return Some((mid, Pos::Right));
                }
                min = mid + 1
            } else if *k < mid_key {
                if mid == 0 {
                    return Some((mid, Pos::Left));
                }
                max = mid - 1
            }
        }

        None
    }

    pub fn insert(&mut self, k: &Data_item, v: &Data_item) -> Result<()> {
        assert_eq!(self.page_type, PageType::LEAF);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                // empty node
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_value_at(0, v)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let page_info = match &self.page_info{
                                None => panic!("not having a pafe info"),
                                Some(page_info) => {
                                    page_info.deref().borrow_mut()
                                }
                            };
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(page_info.keys_pos);
                            let value_ptr = buf_ptr.add(page_info.values_pos);
                            std::ptr::copy(key_ptr.add(i * page_info.key_size), key_ptr.add((i + 1) * page_info.key_size), (old_item_count - i) * page_info.key_size);
                            std::ptr::copy(value_ptr.add(i * page_info.value_size), value_ptr.add((i + 1) * page_info.value_size), (old_item_count - i) * page_info.key_size);
                        }
                        // for j in (i..old_item_count).rev() {
                        //     self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                        //     self.set_value_at(j + 1, &self.value_at(j).unwrap())?;
                        // }
                        self.set_key_at(i, k)?;
                        self.set_value_at(i, v)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let page_info = match &self.page_info{
                                None => panic!("not having a pafe info"),
                                Some(page_info) => {
                                    page_info.deref().borrow_mut()
                                }
                            };
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(page_info.keys_pos);
                            let value_ptr = buf_ptr.add(page_info.values_pos);
                            std::ptr::copy(key_ptr.add((i + 1) * page_info.key_size), key_ptr.add((i + 2) * page_info.key_size), (old_item_count - i - 1) * page_info.key_size);
                            std::ptr::copy(value_ptr.add((i + 1) * page_info.value_size), value_ptr.add((i + 2) * page_info.value_size), (old_item_count - i - 1) * page_info.value_size);
                        }
                        // for j in ((i + 1)..old_item_count).rev() {
                        //     self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                        //     self.set_value_at(j + 1, &self.value_at(j).unwrap())?;
                        // }
                        self.set_key_at(i + 1, k)?;
                        self.set_value_at(i + 1, v)?;
                    }
                }
            }
        }
        self.mark_dirty();
        Ok(())
    }

    pub fn insert_ptr(&mut self, k: &Data_item, ptr: u32) -> Result<()> {
        assert_eq!(self.page_type, PageType::INTERNAL);
        let old_item_count = self.item_count();
        match self.find(k) {
            None => {
                // empty node
                // must first set ptrs[0] !!!
                assert!(self.ptr_at(0).unwrap() > 0);
                self.set_item_count(1)?;
                self.set_key_at(0, k)?;
                self.set_ptr_at(1, ptr)?;
            },
            Some((i, pos)) => {
                match pos {
                    Pos::Current => {
                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Left => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let page_info = match  &self.page_info {
                                None => panic!("not having a page info"),
                                Some(page_info) => {
                                    page_info.deref().borrow_mut()
                                }
                            };
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(page_info.keys_pos);
                            let ptr_ptr = buf_ptr.add(page_info.ptrs_pos);
                            std::ptr::copy(key_ptr.add(i * page_info.key_size), key_ptr.add((i + 1) * page_info.key_size), (old_item_count - i) * page_info.key_size);
                            std::ptr::copy(ptr_ptr.add((i + 1) * PTR_SIZE), ptr_ptr.add((i + 2) * PTR_SIZE ), (old_item_count - i) * PTR_SIZE );
                        }
                        // for j in (i..old_item_count).rev() {
                        //     self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                        //     self.set_ptr_at(j + 2, self.ptr_at(j + 1).unwrap())?;
                        // }
                        self.set_key_at(i, k)?;
                        self.set_ptr_at(i + 1, ptr)?;
                    }
                    Pos::Right => {
                        self.set_item_count(old_item_count + 1)?;
                        unsafe {
                            let page_info = match  &self.page_info {
                                None => panic!("not having a page info"),
                                Some(page_info) => {
                                    page_info.deref().borrow_mut()
                                }
                            };
                            let buf_ptr = self.buf.as_mut_ptr();
                            let key_ptr = buf_ptr.add(page_info.keys_pos);
                            let ptr_ptr = buf_ptr.add(page_info.ptrs_pos);
                            std::ptr::copy(key_ptr.add((i + 1) * page_info.key_size), key_ptr.add((i + 2) * page_info.key_size), (old_item_count - i -1) * page_info.key_size);
                            std::ptr::copy(ptr_ptr.add((i + 2) * PTR_SIZE), ptr_ptr.add((i + 3) * PTR_SIZE), (old_item_count - i - 1) * PTR_SIZE);
                        }
                        // for j in ((i + 1)..old_item_count).rev() {
                        //     self.set_key_at(j + 1, &self.key_at(j).unwrap())?;
                        //     self.set_ptr_at(j + 2, self.ptr_at(j + 1).unwrap())?;
                        // }
                        self.set_key_at(i + 1, k)?;
                        self.set_ptr_at(i + 2, ptr)?;
                    }
                }
            }
        }
        self.mark_dirty();
        Ok(())
    }

    pub fn is_full(& self) -> bool {
        match &self.page_info {
            None => panic!("not have a page info"),
            Some(page_info) => {
                match self.page_type {
                    PageType::INTERNAL => self.item_count() >= page_info.deref().borrow().max_index_count,
                    PageType::LEAF => self.item_count() >= page_info.deref().borrow().max_item_count,
                    _ => panic!("not a meta page")
                }
            }
        }
    }
}

// impl Debug for Page where
// {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         match self.page_type {
//             PageType::META => {
//                 f.write_fmt(format_args!("{:?}; root index:{}; total pages: {}", self.page_type, self.root_index(), self.total_pages()))?;
//             }
//             PageType::LEAF => {
//                 f.write_fmt(format_args!("{:?}; item count:{};\n", self.page_type, self.item_count()))?;
//                 for i in 0..self.item_count() {
//                     f.write_fmt(format_args!("#{} {:?}: {:?}\n", i, self.key_at(i).unwrap(), self.value_at(i).unwrap()))?;
//                 }
//             }
//             PageType::INTERNAL => {
//                 f.write_fmt(format_args!("{:?}; item count:{};\n", self.page_type, self.item_count()))?;
//                 f.write_fmt(format_args!("#_ _: {}\n", self.ptr_at(0).unwrap()))?;
//                 for i in 0..self.item_count() {
//                     f.write_fmt(format_args!("#{} {:?}: {}\n", i, self.key_at(i).unwrap(), self.ptr_at(i + 1).unwrap()))?;
//                 }
//             }
//             PageType::DB_META => {}
//         }
//         Ok(())
//     }
// }

impl Page {
    pub fn sync(&mut self) -> Result<()> {
        if self.dirty {
            let mut fd = self.fd.as_ref().unwrap().as_ref().borrow_mut();
            fd.seek(SeekFrom::Start((self.index * PAGE_SIZE as u32) as u64))?;
            fd.write_all(self.buf.borrow())?;
            self.dirty = false;
        }
        Ok(())
    }
}

impl Drop for Page {
    fn drop(&mut self) {
        println!("{:?}:dirty?:{:?}", self.index,self.dirty);
        if self.dirty == true{
            self.sync().unwrap();
        }
    }
}