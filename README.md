# Mini-LSM

Toy implementation of a log-structured merge (LSM) tree written in Rust. Based roughly off of the [LSM In A Week](https://skyzh.github.io/mini-lsm/00-preface.html) book, with some inspiration also taken from [SarthakMakhija/go-lsm](https://github.com/SarthakMakhija/go-lsm).

## Usage
### Library
The entry point for the store is `mini_lsm::store::LsmStore`:
```
use mini_lsm::{state::storage_state_options::StorageStateOptions, store::LsmStore};

let options = StorageStateOptions::new_with_defaults()?;
let lsm = LsmStore::open(options)?;
...
lsm.close()?;
```
### CLI
```
cargo run
```
to compile and run the CLI tool to interact with an `LsmStore` instance. 

Use `help` to view available operations:
```
$ help
Usage: <COMMAND>

Commands:
  get
  put
  delete
  scan
  fill
  quit
  help    Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```
