mod cli_utils;

use std::{io::Write, ops::Bound, str::from_utf8};

use clap::{Parser, Subcommand};

use mini_lsm::{state::storage_state_options::StorageStateOptions, store::LsmStore};

#[derive(Parser)]
#[clap(name = "", no_binary_name = true)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Get {
        key: String,
    },
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    Scan {
        lower: Option<String>,
        upper: Option<String>,
    },
    Fill {
        lower: u64,
        upper: u64,
    },
    Quit,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = StorageStateOptions::new_with_defaults()?;
    let lsm = LsmStore::open(options)?;
    loop {
        print!("$ ");
        std::io::stdout().flush()?;

        let line = cli_utils::readline()?;
        let args = shlex::split(&line).unwrap_or_default();
        let parsed = Cli::try_parse_from(args);
        if parsed.is_err() {
            parsed.err().unwrap().print()?;
            continue
        }
        match parsed.unwrap().command {
            Command::Get { key } => {
                let value = lsm.get(key.as_bytes())?;
                if let Some(res) = value {
                    println!("{}={}", key, from_utf8(&res)?);
                }
            }
            Command::Put { key, value } => {
                lsm.put(key.as_bytes(), value.as_bytes())?;
            }
            Command::Delete { key } => {
                lsm.delete(key.as_bytes())?;
            }
            Command::Scan { lower, upper } => {
                let lb = lower
                    .as_ref()
                    .map_or(Bound::Unbounded, |v| Bound::Included(v.as_bytes()));
                let ub = upper
                    .as_ref()
                    .map_or(Bound::Unbounded, |v| Bound::Included(v.as_bytes()));
                let iter = lsm.scan(lb, ub)?;
                for kv in iter {
                    println!(
                        "{}={}",
                        from_utf8(&kv.key.get_key())?,
                        from_utf8(&kv.value)?
                    );
                }
            }
            Command::Fill { lower, upper } => {
                for i in lower..upper + 1 {
                    lsm.put(
                        format!("{:?}", i).as_bytes(),
                        format!("value@{:?}", i).as_bytes(),
                    )?;
                }
            }
            Command::Quit => {
                lsm.close()?;
                return Ok(());
            }
        }
    }
}
