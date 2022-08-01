use std::{cmp::Ordering, hash::Hash};

use protos::models as fb_models;
use serde::{Deserialize, Serialize};

use crate::{
    errors::{Error, Result},
    TagKey, TagValue,
};

const TAG_KEY_MAX_LEN: usize = 512;
const TAG_VALUE_MAX_LEN: usize = 4096;

pub fn sort_tags(tags: &mut [Tag]) {
    tags.sort_by(|a, b| -> Ordering { a.key.partial_cmp(&b.key).unwrap() })
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct Tag {
    pub key: TagKey,
    pub value: TagValue,
}

impl Tag {
    pub fn new(key: TagKey, value: TagValue) -> Self {
        Self { key, value }
    }

    pub fn from_flatbuffers(tag: &fb_models::Tag) -> Result<Self> {
        let key = tag
            .key()
            .ok_or(Error::InvalidFlatbufferMessage {
                err: "Tag key cannot be empty".to_string(),
            })?
            .to_vec();
        let value = tag
            .value()
            .ok_or(Error::InvalidFlatbufferMessage {
                err: "Tag value cannot be emptyt".to_string(),
            })?
            .to_vec();
        Ok(Self { key, value })
    }

    pub fn check(&self) -> Result<()> {
        if self.key.is_empty() {
            return Err(Error::InvalidTag {
                err: "Tag key cannot be empty".to_string(),
            });
        }
        if self.value.is_empty() {
            return Err(Error::InvalidTag {
                err: "Tag value cannot be empty".to_string(),
            });
        }
        if self.key.len() > TAG_KEY_MAX_LEN {
            return Err(Error::InvalidTag {
                err: format!("Tag key exceeds the TAG_KEY_MAX_LEN({})", TAG_KEY_MAX_LEN),
            });
        }
        if self.value.len() > TAG_VALUE_MAX_LEN {
            return Err(Error::InvalidTag {
                err: format!(
                    "Tag value exceeds the TAG_VALUE_MAX_LEN({})",
                    TAG_VALUE_MAX_LEN
                ),
            });
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.key.len() + self.value.len());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }
}

pub trait TagFromParts<T1, T2> {
    fn from_parts(key: T1, value: T2) -> Self;
}

impl TagFromParts<&str, &str> for Tag {
    fn from_parts(key: &str, value: &str) -> Self {
        Tag {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }
}

impl TagFromParts<TagKey, TagValue> for Tag {
    fn from_parts(key: TagKey, value: TagValue) -> Self {
        Tag { key, value }
    }
}

#[cfg(test)]
mod tests_tag {
    use crate::Tag;

    #[test]
    fn test_tag_bytes() {
        let tag = Tag::new(b"hello".to_vec(), b"123".to_vec());
        assert_eq!(tag.to_bytes(), Vec::from("hello123"));
    }

    #[test]
    fn test_tag_format_check() {
        let tag = Tag::new(b"hello".to_vec(), b"123".to_vec());
        tag.check().unwrap();
    }
}
