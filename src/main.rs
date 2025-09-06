use std::sync::Arc;
use std::thread;
mod snowflake;
use snowflake::SnowflakeGenerator;

fn main() {
    println!("开始生成 Snowflake ID...");

    // 创建一个线程安全的 Snowflake 生成器实例
    // 在实际应用中，worker_id 和 datacenter_id 需要从配置中读取
    let generator = match SnowflakeGenerator::new(1, 1) {
        Ok(g) => Arc::new(g),
        Err(e) => {
            eprintln!("创建 Snowflake 生成器失败: {}", e);
            return;
        }
    };

    let mut handles = vec![];

    // 在主线程中生成 ID
    println!("--- 主线程 ID ---");
    for i in 0..5 {
        match generator.next_id() {
            Ok(id) => println!("[{}] ID: {}", i + 1, id),
            Err(e) => eprintln!("生成 ID 时出错: {}", e),
        }
    }

    // 在多个线程中并发生成 ID 以测试并发性
    println!("\n--- 并发线程 ID ---");
    for i in 0..3 {
        let gen_clone = Arc::clone(&generator);
        let handle = thread::spawn(move || {
            for j in 0..5 {
                match gen_clone.next_id() {
                    Ok(id) => println!("[线程 {} - {}] ID: {}", i, j + 1, id),
                    Err(e) => eprintln!("[线程 {}] 生成 ID 时出错: {}", i, e),
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("\nSnowflake ID 生成演示结束。");
}
