use anyhow::{anyhow, Result};
use core::mem;
use crate::db_str::Db_str;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum DATATYPE{
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    USIZE(usize),
    ISIZE(isize),
    F32(f32),
    F64(f64),
    STR(Db_str)
}

pub fn toDATATYPE(mut vec: Vec<u8>, datatype: &mut DATATYPE){
    match datatype {
        DATATYPE::U8(content) => unsafe {
            let vec_ptr = vec.as_ptr();
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::U16(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const u16;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::U32(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const u32;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::U64(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const u64;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::I8(content) => unsafe {
        let vec_ptr = vec.as_ptr() as *const i8;
        let res = std::ptr::read(vec_ptr);
        *content = res;
        },
        DATATYPE::I16(content) => unsafe {
        let vec_ptr = vec.as_ptr() as *const i16;
        let res = std::ptr::read(vec_ptr);
        *content = res;
        },
        DATATYPE::I32(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const i32;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::I64(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const i64;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::USIZE(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const usize;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::ISIZE(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const isize;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::F32(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const f32;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::F64(content) => unsafe {
            let vec_ptr = vec.as_ptr() as *const f64;
            let res = std::ptr::read(vec_ptr);
            *content = res;
        },
        DATATYPE::STR(content) => {
            while vec.len() > 0 && vec[vec.len() - 1] == 0 {
                vec.pop();
            }
            content.str = String::from_utf8(vec).unwrap();
        },
    }
    ;
}

pub fn get_u32(buf: &[u8]) -> u32 {
    let mut type_u32 = DATATYPE::U32(0);
    toDATATYPE(type_u32.decode(&buf[..]),& mut type_u32);
    let type_u32_value = match type_u32 { DATATYPE::U32(value) => value , _ => panic!("can't happen") };
    type_u32_value
}
//提供u32的封装
pub fn get_db_str(buf: &[u8], size: usize) -> Db_str {
    let mut type_db_str = DATATYPE::STR(Db_str::new_container(size));
    toDATATYPE(type_db_str.decode(buf),& mut type_db_str);
    let type_db_str_value = match type_db_str { DATATYPE::STR(value) => value , _ => panic!("can't happen") };
    type_db_str_value
}
//提供db_str的封装

pub fn get_type(buf: &[u8]) -> DATATYPE {
    let mut type_u32 = DATATYPE::U32(0);
    toDATATYPE(type_u32.decode(&buf[..]),& mut type_u32);
    let type_u32_value = match type_u32 { DATATYPE::U32(value) => value , _ => panic!("can't happen") };
    match  type_u32_value {
        0 => DATATYPE::U8(0),
        1 => DATATYPE::U16(0),
        2 => DATATYPE::U32(0),
        3 => DATATYPE::U64(0),
        4 => DATATYPE::I8(0),
        5 => DATATYPE::I16(0),
        6 => DATATYPE::I32(0),
        7 => DATATYPE::I64(0),
        8 => DATATYPE::USIZE(0),
        9 => DATATYPE::ISIZE(0),
        10 => DATATYPE::F32(0.0),
        11 => DATATYPE::F64(0.0),
        value => DATATYPE::STR(Db_str::new_container(value as usize - 11)),
    }
}

pub fn set_type(buf: &mut [u8],datatype: & DATATYPE){
    let type_num:u32 = match datatype {
        DATATYPE::U8(_) => 0,
        DATATYPE::U16(_) => 1,
        DATATYPE::U32(_) => 2,
        DATATYPE::U64(_) => 3,
        DATATYPE::I8(_) => 4,
        DATATYPE::I16(_) => 5,
        DATATYPE::I32(_) => 6,
        DATATYPE::I64(_) => 7,
        DATATYPE::USIZE(_) => 8,
        DATATYPE::ISIZE(_) =>9,
        DATATYPE::F32(_) => 10,
        DATATYPE::F64(_) => 11,
        DATATYPE::STR(value) => value.capacity as u32 + 11
    };
    type_num.encode(buf);
}

impl DATATYPE{
    pub fn type_equal(&self, other: & DATATYPE) -> bool {
        match self {
            DATATYPE::U8(_) => {
                match other {
                    DATATYPE::U8(_) => true,
                    _ => false
                }
            }
            DATATYPE::U16(_) => {
                match other {
                    DATATYPE::U16(_) => true,
                    _ => false
                }
            }
            DATATYPE::U32(_) => {
                match other {
                    DATATYPE::U32(_) => true,
                    _ => false
                }
            }
            DATATYPE::U64(_) => {
                match other {
                    DATATYPE::U64(_) => true,
                    _ => false
                }
            }
            DATATYPE::I8(_) => {
                match other {
                    DATATYPE::I8(_) => true,
                    _ => false
                }
            }
            DATATYPE::I16(_) => {
                match other {
                    DATATYPE::I16(_) => true,
                    _ => false
                }
            }
            DATATYPE::I32(_) => {
                match other {
                    DATATYPE::I32(_) => true,
                    _ => false
                }
            }
            DATATYPE::I64(_) => {
                match other {
                    DATATYPE::I64(_) => true,
                    _ => false
                }
            }
            DATATYPE::USIZE(_) => {
                match other {
                    DATATYPE::USIZE(_) => true,
                    _ => false
                }
            }
            DATATYPE::ISIZE(_) => {
                match other {
                    DATATYPE::ISIZE(_) => true,
                    _ => false
                }
            }
            DATATYPE::F32(_) => {
                match other {
                    DATATYPE::F32(_) => true,
                    _ => false
                }
            }
            DATATYPE::F64(_) => {
                match other {
                    DATATYPE::F64(_) => true,
                    _ => false
                }
            }
            DATATYPE::STR(self_content) => {
                match other {
                    DATATYPE::STR(content) => {
                        if self_content.capacity == content.capacity{
                            true
                        }
                        else{
                            false
                        }
                    }
                    _ => false
                }
            }
        }
    }
}

impl Attribute for DATATYPE{
    fn bin_size(&self) -> usize {
        match self {
            DATATYPE::U8(content) => content.bin_size(),
            DATATYPE::U16(content) => content.bin_size(),
            DATATYPE::U32(content) => content.bin_size(),
            DATATYPE::U64(content) => content.bin_size(),
            DATATYPE::I8(content) => content.bin_size(),
            DATATYPE::I16(content) => content.bin_size(),
            DATATYPE::I32(content) => content.bin_size(),
            DATATYPE::I64(content) => content.bin_size(),
            DATATYPE::USIZE(content) => content.bin_size(),
            DATATYPE::ISIZE(content) => content.bin_size(),
            DATATYPE::F32(content) => content.bin_size(),
            DATATYPE::F64(content) => content.bin_size(),
            DATATYPE::STR(content) => content.bin_size()
        }
    }

    fn encode(&self, buf: &mut [u8]) {
        match self {
            DATATYPE::U8(content) => content.encode(buf),
            DATATYPE::U16(content) => content.encode(buf),
            DATATYPE::U32(content) => content.encode(buf),
            DATATYPE::U64(content) => content.encode(buf),
            DATATYPE::I8(content) => content.encode(buf),
            DATATYPE::I16(content) => content.encode(buf),
            DATATYPE::I32(content) => content.encode(buf),
            DATATYPE::I64(content) => content.encode(buf),
            DATATYPE::USIZE(content) => content.encode(buf),
            DATATYPE::ISIZE(content) => content.encode(buf),
            DATATYPE::F32(content) => content.encode(buf),
            DATATYPE::F64(content) => content.encode(buf),
            DATATYPE::STR(content) => content.encode(buf)
        }
    }

    fn decode(&self, buf: &[u8]) -> Vec<u8> {
        match self {
            DATATYPE::U8(content) => content.decode(buf),
            DATATYPE::U16(content) => content.decode(buf),
            DATATYPE::U32(content) => content.decode(buf),
            DATATYPE::U64(content) => content.decode(buf),
            DATATYPE::I8(content) => content.decode(buf),
            DATATYPE::I16(content) => content.decode(buf),
            DATATYPE::I32(content) => content.decode(buf),
            DATATYPE::I64(content) => content.decode(buf),
            DATATYPE::USIZE(content) => content.decode(buf),
            DATATYPE::ISIZE(content) => content.decode(buf),
            DATATYPE::F32(content) => content.decode(buf),
            DATATYPE::F64(content) => content.decode(buf),
            DATATYPE::STR(content) => content.decode(buf)
        }
    }
}

pub trait Attribute {
    fn bin_size(&self) -> usize;

    fn encode(&self, buf: &mut [u8]);

    fn decode(&self, buf: &[u8]) -> Vec<u8>;
}

pub fn check_len(buf: &[u8], size: usize){
    if buf.len() < size {
        panic!("check_len buf too short")
    }
}

macro_rules! num_impl {
    ($ty: ty, $size: tt) => {
        impl Attribute  for $ty {
            #[inline]
            fn bin_size(&self) -> usize {
                $size
            }
            fn encode(&self, buf: &mut [u8]){
                check_len(buf, $size);
                let ptr = buf.as_mut_ptr() as *mut $ty;
                unsafe {
                    std::ptr::write(ptr, self.clone());
                    // assert_eq!(std::ptr::read(y), 12);
                }
                // unsafe { *(&mut buf[0] as *mut _ as *mut _) = self.to_be() };
            }
            fn decode(&self, buf: &[u8]) -> Vec<u8> {
                check_len(buf, $size);
                let mut res = Vec::new();
                res.extend_from_slice(&buf[..$size]);
                res
            }
        }
    }
}

num_impl!(u8, 1);
num_impl!(u16, 2);
num_impl!(u32, 4);
num_impl!(u64, 8);
num_impl!(i8, 1);
num_impl!(i16, 2);
num_impl!(i32, 4);
num_impl!(i64, 8);
num_impl!(usize, (mem::size_of::<usize>()));
num_impl!(isize, (mem::size_of::<isize>()));
num_impl!(f32, 4);
num_impl!(f64, 8);
#[cfg(test)]
mod test{
    use crate::{Attribute, DATATYPE};
    use crate::db_str::Db_str;
    use crate::data_item::{Data_item_info, Data_item};

    #[test]
    fn test_type(){
        // DATA_TYPE::F32(3.5)
        // println!("{}",3.5.bin_size());
        let mut vec = vec![0u8; 20];
        3.4.encode(&mut vec[..]);
        println!("{:?}",vec);
        let x = 34655;
        x.encode(&mut vec[8..]);
        println!("{:?}",vec);
        let db_str = Db_str::new("ewrwere",8);
        db_str.encode(&mut vec[12..]);
        println!("{:?}",vec);
    }
    #[test]
    fn test_data_item(){
        let mut vec = vec![0u8; 30];
        let mut row_with_attribute_name = Data_item_info::new();
        row_with_attribute_name.add((DATATYPE::F64(5.6),String::from("f64_attribute")));
        row_with_attribute_name.add((DATATYPE::U32(7),String::from("u32_attribute")));
        row_with_attribute_name.add((DATATYPE::STR(Db_str::new_container(10)),String::from("str_attribute")));
        let record_row = row_with_attribute_name.get_data_item();
        record_row.encode(&mut vec);
        println!("{:?}",vec);
        // row_with_attribute_name.set(DATATYPE::F64(13.3),"f6_attribute");
        row_with_attribute_name.set(DATATYPE::F64(13.3),"f64_attribute");
        row_with_attribute_name.set(DATATYPE::U32(9),"u32_attribute");
        // row_with_attribute_name.clear_value();
        row_with_attribute_name.set(DATATYPE::STR(Db_str::new("test",10)),"str_attribute");
        // let record_row = row_with_attribute_name.get_data_item();
        let record_row = row_with_attribute_name.get_data_item();
        record_row.encode(&mut vec);
        println!("{:?}",vec);
    }
}