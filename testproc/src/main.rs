use std::thread;
use std::time::Duration;

fn main() {
    println!("test proc activated! sleeping");
    loop { thread::sleep(Duration::from_millis(60000)); }
}
