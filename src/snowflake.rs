use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// 常量定义
const EPOCH: u64 = 1609459200000; // 自定义纪元时间 (2021-01-01 00:00:00 UTC)
const WORKER_ID_BITS: u64 = 5;
const DATACENTER_ID_BITS: u64 = 5;
const SEQUENCE_BITS: u64 = 12;

// 各部分的最大值
const MAX_WORKER_ID: u64 = (1 << WORKER_ID_BITS) - 1;
const MAX_DATACENTER_ID: u64 = (1 << DATACENTER_ID_BITS) - 1;
const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;

// 位移量
const TIMESTAMP_SHIFT: u64 = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;
const DATACENTER_ID_SHIFT: u64 = SEQUENCE_BITS + WORKER_ID_BITS;
const WORKER_ID_SHIFT: u64 = SEQUENCE_BITS;

/// Snowflake 核心结构体
pub struct Snowflake {
    last_timestamp: u64,
    sequence: u64,
    worker_id: u64,
    datacenter_id: u64,
}

impl Snowflake {
    /// 创建一个新的 Snowflake 实例
    pub fn new(worker_id: u64, datacenter_id: u64) -> Result<Self, &'static str> {
        if worker_id > MAX_WORKER_ID || datacenter_id > MAX_DATACENTER_ID {
            return Err("Worker ID or Datacenter ID is out of range");
        }
        Ok(Self {
            last_timestamp: 0,
            sequence: 0,
            worker_id,
            datacenter_id,
        })
    }

    /// 生成下一个唯一 ID
    pub fn next_id(&mut self) -> Result<u64, &'static str> {
        let mut timestamp = Self::current_timestamp();

        if timestamp < self.last_timestamp {
            return Err("Clock moved backwards. Refusing to generate id.");
        }

        if timestamp == self.last_timestamp {
            self.sequence = (self.sequence + 1) & MAX_SEQUENCE;
            if self.sequence == 0 {
                timestamp = self.til_next_millis(self.last_timestamp);
            }
        } else {
            self.sequence = 0;
        }

        self.last_timestamp = timestamp;

        let id = ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
            | (self.datacenter_id << DATACENTER_ID_SHIFT)
            | (self.worker_id << WORKER_ID_SHIFT)
            | self.sequence;

        Ok(id)
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64
    }

    fn til_next_millis(&self, last_timestamp: u64) -> u64 {
        let mut timestamp = Self::current_timestamp();
        while timestamp <= last_timestamp {
            timestamp = Self::current_timestamp();
        }
        timestamp
    }
}

/// 用于线程安全访问的包装器
pub struct SnowflakeGenerator {
    mutex: Mutex<Snowflake>,
}

impl SnowflakeGenerator {
    pub fn new(worker_id: u64, datacenter_id: u64) -> Result<Self, &'static str> {
        let snowflake = Snowflake::new(worker_id, datacenter_id)?;
        Ok(Self {
            mutex: Mutex::new(snowflake),
        })
    }

    pub fn next_id(&self) -> Result<u64, &'static str> {
        // 加锁以保证线程安全
        self.mutex.lock().unwrap().next_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_new_with_valid_ids() {
        assert!(Snowflake::new(0, 0).is_ok());
        assert!(Snowflake::new(MAX_WORKER_ID, MAX_DATACENTER_ID).is_ok());
    }

    #[test]
    fn test_new_with_invalid_ids() {
        assert!(Snowflake::new(MAX_WORKER_ID + 1, 0).is_err());
        assert!(Snowflake::new(0, MAX_DATACENTER_ID + 1).is_err());
    }

    #[test]
    fn test_next_id_uniqueness() {
        let mut snowflake = Snowflake::new(1, 1).unwrap();
        let mut ids = HashSet::new();
        let num_ids = 10000;

        for _ in 0..num_ids {
            let id = snowflake.next_id().unwrap();
            assert!(ids.insert(id), "生成了重复的 ID: {}", id);
        }
        assert_eq!(ids.len(), num_ids);
    }

    #[test]
    fn test_id_structure() {
        let worker_id = 5;
        let datacenter_id = 10;
        let mut snowflake = Snowflake::new(worker_id, datacenter_id).unwrap();
        let id = snowflake.next_id().unwrap();

        let decoded_worker_id = (id >> WORKER_ID_SHIFT) & MAX_WORKER_ID;
        let decoded_datacenter_id = (id >> DATACENTER_ID_SHIFT) & MAX_DATACENTER_ID;
        let decoded_timestamp = (id >> TIMESTAMP_SHIFT) + EPOCH;

        assert_eq!(decoded_worker_id, worker_id);
        assert_eq!(decoded_datacenter_id, datacenter_id);
        
        let current_ts = Snowflake::current_timestamp();
        // 允许几毫秒的误差
        assert!(decoded_timestamp <= current_ts && current_ts - decoded_timestamp < 50);
    }
    
    #[test]
    fn test_concurrent_generation_uniqueness() {
        let generator = Arc::new(SnowflakeGenerator::new(1, 1).unwrap());
        let mut handles = vec![];
        let num_threads = 10;
        let ids_per_thread = 1000;

        for _ in 0..num_threads {
            let gen_clone = Arc::clone(&generator);
            let handle = thread::spawn(move || {
                let mut thread_ids = Vec::new();
                for _ in 0..ids_per_thread {
                    thread_ids.push(gen_clone.next_id().unwrap());
                }
                thread_ids
            });
            handles.push(handle);
        }

        let mut all_ids = HashSet::new();
        for handle in handles {
            let thread_ids = handle.join().unwrap();
            for id in thread_ids {
                assert!(all_ids.insert(id), "并发生成时出现重复 ID: {}", id);
            }
        }
        assert_eq!(all_ids.len(), num_threads * ids_per_thread);
    }
}