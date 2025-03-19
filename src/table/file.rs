use std::{io::Read, path::Path};

use anyhow::Result;
pub struct File {
    file: std::fs::File,
    size: usize
}

impl File {
    pub fn create(path: impl AsRef<Path>, data: Vec<u8>) -> Result<Self> {
        std::fs::write(&path, &data)?;
        Ok(Self {
            file: std::fs::File::open(path)?,  // read-only mode
            size: data.len(),
        })
    }

    pub fn get_contents_as_bytes(&mut self) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::new();
        self.file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub fn get_size(&self) -> usize {
        self.size
    }
}