use anyhow::{anyhow, Result};
use crate::{Attribute, check_len, DATATYPE, toDATATYPE};
use std::ops::Deref;
use std::cmp::Ordering;
use std::fs::read;

#[derive(Debug)]
pub struct Data_item{
    pub attributes: Vec<(DATATYPE,bool)>
}

impl Clone for Data_item{
    fn clone(&self) -> Self {
        let attributes = self.attributes.clone();
        Data_item{
            attributes
        }
    }
}

impl PartialEq for Data_item{
    fn eq(&self, other: &Self) -> bool {
        for (index, attribute) in self.attributes.iter().enumerate() {
            if attribute.0.eq(&other.attributes[index].0 ) {
            }else {
                return false;
            }
        }
        true
    }
}

impl PartialOrd for Data_item{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        for (index, attribute) in self.attributes.iter().enumerate() {
            match &attribute.0.partial_cmp(&other.attributes[index].0 ){
                None => {}
                Some(res) => {
                    match res {
                        Ordering::Less => return Some(Ordering::Less),
                        Ordering::Equal => {}
                        Ordering::Greater => return Some(Ordering::Greater),
                    }
                }
            }
        }
        Some(Ordering::Equal)
    }
}

impl Data_item{
    // pub fn decode_by_name(&self,str: &str,buf: &[u8]) -> Option<Box<Attribute>>{
    //     let mut bin_size = 0;
    //     for attribute in self.attributes.iter(){
    //         if attribute.1 == str {
    //             return Some(Box::new(attribute.0.deref().bin_size()))
    //         }
    //         bin_size += attribute.0.deref().bin_size();
    //     }
    //     None
    // }
    pub fn from_vec(&self, mut vec: Vec<u8>) -> Data_item {
        assert_eq!(vec.len(), self.bin_size());
        let mut res = self.clone();
        let mut bin_size = 0;
        let mut i = 0;
        while i < self.attributes.len() {
            let vec_temp = vec.split_off(res.attributes[i].0.bin_size());
            toDATATYPE(vec, &mut res.attributes[i].0);
            vec = vec_temp;
            i = i + 1;
        }
        res
    }
}

impl Attribute for Data_item{
    fn bin_size(&self) -> usize {
        let mut bin_size = 0;
        for attribute in self.attributes.iter() {
            bin_size += &attribute.0.bin_size();
        }
        bin_size
    }

    fn encode(&self, buf: &mut [u8]) {
        check_len(buf, self.bin_size());
        let mut bin_size = 0;
        for attribute in self.attributes.iter() {
            if attribute.1 {
                &attribute.0.encode(&mut buf[bin_size..]);
            }
            bin_size += &attribute.0.bin_size();
        }
    }

    fn decode(&self, buf: &[u8]) -> Vec<u8> {
        let mut res = Vec::new();
        let mut bin_size = 0;
        for attribute in self.attributes.iter() {
            res.append(&mut attribute.0.decode(&buf[bin_size..]));
            bin_size += &attribute.0.bin_size();
        }
        res
    }

    // fn encode(&self, buf: &mut [u8]) -> Result<()> {
    //     check_len(buf, self.bin_size())?;
    //     let mut bin_size = 0;
    //     for attribute in self.attributes.iter() {
    //         &attribute.0.encode(&mut buf[bin_size..]);
    //         bin_size += &attribute.0.bin_size();
    //     }
    // }

    // fn decode(&self, buf: &[u8]) -> Result<(Box<Self>, usize)> {
    //     // unimplemented!()
    //     let mut res = self.clone();
    //     let mut bin_size = 0;
    //     for (index, attribute) in res.attributes.iter().enumerate(){
    //          res.attributes[index].0 = attribute.0.deref().decode(&buf[bin_size..]).0;
    //         bin_size += attribute.0.deref().bin_size();
    //     }
    //     Ok((Box::new(res),bin_size))
    // }
}

// #[derive(Debug)]
// pub struct Update_data_item{
//     pub attributes: Vec<(DATATYPE,bool)>
// }
//
// impl Attribute for Update_data_item{
//     fn bin_size(&self) -> usize {
//         let mut bin_size = 0;
//         for attribute in self.attributes.iter() {
//             bin_size += &attribute.0.bin_size();
//         }
//         bin_size
//     }
//
//     fn encode(&self, buf: &mut [u8]) {
//         check_len(buf, self.bin_size());
//         let mut bin_size = 0;
//         for attribute in self.attributes.iter() {
//             if attribute.1 {
//                 &attribute.0.encode(&mut buf[bin_size..]);
//             }
//             bin_size += &attribute.0.bin_size();
//         }
//     }
//
//     fn decode(&self, buf: &[u8]) -> Vec<u8> {
//         let mut res = Vec::new();
//         let mut bin_size = 0;
//         for attribute in self.attributes.iter() {
//             res.append(&mut attribute.0.decode(&buf[bin_size..]));
//             bin_size += &attribute.0.bin_size();
//         }
//         res
//     }
// }

#[derive(Debug)]
pub struct Data_item_info{
    pub attributes: Vec<(DATATYPE,String,bool)>
}

impl Clone for Data_item_info{
    fn clone(&self) -> Self {
        let attributes = self.attributes.clone();
        Data_item_info{
            attributes
        }
    }
}

impl Data_item_info{
    pub fn new() -> Data_item_info{
        Data_item_info{
            attributes: Vec::new()
        }
    }

    pub fn add(&mut self, attribute: (DATATYPE, String)){
        self.attributes.push((attribute.0,attribute.1,false));
    }

    pub fn set(&mut self, value: DATATYPE, attributename: &str) -> bool {
        for mut attribute in &mut self.attributes {
            // println!("attribute:{:?}",attribute);
            if  attribute.1 == attributename {
                if attribute.0.type_equal(&value) {
                    attribute.0 = value;
                    attribute.2 = true;
                    return true;
                } else {
                    return false;}
            }
        }
        // println!("555");
        false
    }

    pub fn get_data_item(&self) -> Data_item{
        let mut attributes = Vec::new();
        for attribute in &self.attributes {
            attributes.push((attribute.0.clone(),attribute.2));
        }
        Data_item{
            attributes
        }
    }

    // pub fn get_update_data_item(&self) -> Update_data_item{
    //     let mut attributes = Vec::new();
    //     for attribute in &self.attributes {
    //         attributes.push((attribute.0.clone(),attribute.2));
    //     }
    //     Update_data_item{
    //         attributes
    //     }
    // }

    pub fn clear_value(&mut self) -> &mut Data_item_info {
        for attribute in &mut self.attributes {
            attribute.2 = false;
        }
        self
    }
}