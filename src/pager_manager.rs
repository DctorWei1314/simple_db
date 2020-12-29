extern crate lru;

use lru::LruCache;
use crate::page::{Page, PageType, PAGE_SIZE, PageError};
use std::cell::{RefCell, RefMut};
use std::rc::Rc;
use std::iter::Map;
use crate::BTree;
use std::fs::{File, OpenOptions};
use anyhow::{Result, Error};
use std::borrow::BorrowMut;
use std::io::{Seek, SeekFrom, Read};
use std::ops::Deref;

const LRU_CACHE_SIZE: usize = 50;

#[derive(Debug)]
pub struct Pager_manager{
    lru:LruCache<u32,Rc<RefCell<Page>>>,
    pub db_meta_page: Rc<RefCell<Page>>,
    fd: Rc<RefCell<File>>
}


impl Pager_manager{
    pub fn new(path: &str) -> Pager_manager{
        let lru = LruCache::<u32,Rc<RefCell<Page>>>::new(LRU_CACHE_SIZE);
        let mut fd = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path).expect("could not open btree file");
        let rc_fd = Rc::new(RefCell::new(fd));
        let db_meta_page;
        if rc_fd.deref().borrow_mut().metadata().unwrap().len() == 0 {
            db_meta_page = Rc::new(RefCell::new(Pager_manager::init_as_empty(rc_fd.clone())));
        } else {
            db_meta_page = Rc::new(RefCell::new(Pager_manager::init_load(rc_fd.clone())));
        }
        Pager_manager{
            lru,
            db_meta_page,
            fd: rc_fd
        }
    }

    pub fn init_as_empty(fd: Rc<RefCell<File>>) -> Page {
        println!("init empty db");
        let mut db_meta_page = Page::new(fd, 0, PageType::DB_META).unwrap();
        db_meta_page.sync();
        db_meta_page
    }

    pub fn init_load(fd: Rc<RefCell<File>>) -> Page {
        println!("load empty db");
        let mut page = Page::default();

        {
            let mut _fd = fd.deref().borrow_mut();
            _fd.seek(SeekFrom::Start((page.index as usize * PAGE_SIZE) as u64));
            _fd.read_exact(page.buf.borrow_mut());
        }
        page.page_type = page.get_page_type();
        assert_eq!(page.page_type, PageType::DB_META);
        page.fd = Some(fd);
        // page.init_layout();
        page
    }

    pub fn new_page(&mut self, pt: PageType) -> Result<Rc<RefCell<Page>>> {
        let mut meta_page = self.db_meta_page.deref().borrow_mut();
        let max_index = meta_page.total_pages();
        let res = Page::new(self.fd.clone(), max_index, pt)?;
        self.lru.put(max_index, Rc::new(RefCell::new(res)));
        meta_page.set_total_page(max_index + 1);
        Ok(self.lru.get(&max_index).unwrap().clone())
    }

    fn load_page(&mut self, index: u32) -> Result<Page> {
        if index >= self.db_meta_page.deref().borrow().total_pages() {
            panic!("load wrong!!")
        }
        let mut page = Page::default();

        {
            let mut _fd = self.fd.deref().borrow_mut();
            page.index = index;
            _fd.seek(SeekFrom::Start((index as usize * PAGE_SIZE) as u64))?;
            _fd.read_exact(page.buf.borrow_mut())?;
        }

        page.page_type = page.get_page_type();
        page.fd = Some(self.fd.clone());
        // page.init_layout();
        Ok(page)
    }

    pub fn get_page(&mut self, index: u32) -> Result<Rc<RefCell<Page>>>{
        if index == 0 {
            return Ok(self.db_meta_page.clone());
        }
        let res = self.lru.get(&index);
        match res {
            Some(res) => {
                Ok(res.clone())
            }
            None => {
                match  self.load_page(index){
                    Ok(res) => {
                        let index = res.index;
                        self.lru.put(res.index, Rc::new(RefCell::new(res)));
                        Ok(self.lru.get(&index).unwrap().clone())
                    }
                    Err(err) => {panic!("err:{}" , err)}
                }
            }
        }
    }
}