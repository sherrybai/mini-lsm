use anyhow::Result;

pub fn readline() -> Result<String> {
    let mut buffer = String::new();
    std::io::stdin()
        .read_line(&mut buffer)?;
    Ok(buffer)
}