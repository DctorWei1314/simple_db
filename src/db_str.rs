use crate::{check_len, Attribute};
use anyhow::{anyhow, Result};
use core::ptr;
use std::cmp::Ordering;

#[derive(Debug, Clone)]
pub struct Db_str{
    pub capacity:usize,
    pub str:String
}

impl PartialEq for Db_str{
    fn eq(&self, other: &Self) -> bool {
        self.str.eq(&other.str)
    }
}

impl PartialOrd for Db_str{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.str.partial_cmp(&other.str)
    }
}


impl Db_str {
    pub fn new(str: &str, capacity:usize) -> Self{
        Db_str{
            capacity,
            str:String::from(str)
        }
    }

    pub fn new_container(capacity:usize) -> Self{
        Db_str{
            capacity,
            str:String::new()
        }
    }
}

impl Attribute for Db_str {
    fn bin_size(&self) -> usize {
        self.capacity
    }
    fn encode(&self, buf: &mut [u8]) {
        check_len(buf, self.capacity);
        let bytes = self.str.as_bytes();
        // println!("{:?}{:?}", bytes.len(), self.capacity);
        if bytes.len() >= self.capacity{
            panic!("string to long");
        }
        let p = buf.as_mut_ptr();
        let mut  i:isize = bytes.len() as isize;
        unsafe {
            ptr::copy(bytes.as_ptr(), p,  bytes.len());
            // ptr::write(p.offset(i), bytes[i]);
            // std::ptr::copy_nonoverlapping(bytes.as_ptr(), &mut buf[0], bytes.len());
        }
        while  i < self.capacity as isize {
            unsafe {
                ptr::write(p.offset(i), 0);
                // std::ptr::copy_nonoverlapping(bytes.as_ptr(), &mut buf[0], bytes.len());
            }
            i = i + 1;
        }
    }

    fn decode(&self, buf: &[u8]) -> Vec<u8> {
        let mut str_end_i = 0;
        for i in 0.. {
            if buf[i] == 0 {
                str_end_i = i;
                break;
            }
        }
        let mut res = Vec::new();
        res.extend_from_slice(&buf[..str_end_i]);
        res.resize(self.capacity,0);
        res
        // let s = std::str::from_utf8(&buf[..str_end_i])?;
        // Ok((Box::new(Db_str::new(s,self.capacity)),str_end_i))
        // capacity 无用
    }


    // fn encode(&self, buf: &mut [u8]) -> Result<usize> {
    //     check_len(buf, self.capacity)?;
    //     let bytes = self.str.as_bytes();
    //     if bytes.len() < self.capacity - 1 {
    //         unsafe {
    //             std::ptr::copy_nonoverlapping(bytes.as_ptr(), &mut buf[0], bytes.len());
    //         }
    //         buf[bytes.len()] = 0;
    //     }
    //     else {
    //         return Err(anyhow!("string{} too long{}", bytes.len(), self.capacity - 1))
    //     }
    //     // std::ptr::copy_nonoverlapping(bytes, buf, bytes.len());
    //     // std::io::Write::write(buf, self.0.as_bytes())?;
    //     Ok(bytes.len())
    // }
    //
    // fn decode(&self, buf: &[u8]) -> anyhow::Result<(Box<Self>,usize)> {
    //     let mut str_end_i = 0;
    //     for i in 0.. {
    //         if buf[i] == 0 {
    //             str_end_i = i;
    //             break;
    //         }
    //     }
    //     let s = std::str::from_utf8(&buf[..str_end_i])?;
    //     Ok((Box::new(Db_str::new(s,self.capacity)),str_end_i))
    //     // capacity 无用
    // }
}