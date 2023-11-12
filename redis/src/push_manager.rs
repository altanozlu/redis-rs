use crate::{PushKind, RedisResult, Value};
use arc_swap::ArcSwap;
use std::sync::Arc;

/// Holds information about received Push data
#[derive(Debug, Clone)]
pub struct PushInfo {
    /// Push Kind
    pub kind: PushKind,
    /// Data from push message
    pub data: Vec<Value>,

    /// Connection address to distinguish connections
    pub con_addr: Arc<String>,
}

/// Manages Push messages for both tokio and std channels
#[derive(Clone, Default)]
pub struct PushManager {
    sender: Arc<ArcSwap<Option<tokio::sync::mpsc::UnboundedSender<PushInfo>>>>,
}
impl PushManager {
    /// It checks if value's type is Push
    /// then invokes `try_send_raw` method
    pub(crate) fn try_send(&self, value: &RedisResult<Value>, con_addr: &Arc<String>) {
        if let Ok(value) = &value {
            self.try_send_raw(value, con_addr);
        }
    }

    /// It checks if value's type is Push and there is a provided sender
    /// then creates PushInfo and invokes `send` method of sender
    pub(crate) fn try_send_raw(&self, value: &Value, con_addr: &Arc<String>) {
        if let Value::Push { kind, data } = value {
            let guard = self.sender.load();
            if let Some(sender) = guard.as_ref() {
                let push_info = PushInfo {
                    kind: kind.clone(),
                    data: data.clone(),
                    con_addr: con_addr.clone(),
                };
                if sender.send(push_info).is_err() {
                    self.sender.compare_and_swap(guard, Arc::new(None));
                }
            }
        }
    }
    /// Replace mpsc channel of `PushManager` with provided sender.
    pub fn replace_sender(&self, sender: tokio::sync::mpsc::UnboundedSender<PushInfo>) {
        self.sender.store(Arc::new(Some(sender)));
    }

    /// Creates new `PushManager`
    pub fn new() -> Self {
        PushManager {
            sender: Arc::from(ArcSwap::from(Arc::new(None))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_manager() {
        let push_manager = PushManager::new();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        push_manager.replace_sender(tx);

        let con_addr = Arc::new("127.0.0.1:6379".to_string());
        let value = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Data("hello".to_string().into_bytes())],
        });

        push_manager.try_send(&value, &con_addr);

        let push_info = rx.try_recv().unwrap();
        assert_eq!(push_info.kind, PushKind::Message);
        assert_eq!(
            push_info.data,
            vec![Value::Data("hello".to_string().into_bytes())]
        );
        assert_eq!(*push_info.con_addr, "127.0.0.1:6379".to_string());
    }
    #[test]
    fn test_push_manager_receiver_dropped() {
        let push_manager = PushManager::new();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        push_manager.replace_sender(tx);

        let con_addr = Arc::new("127.0.0.1:6379".to_string());
        let value = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Data("hello".to_string().into_bytes())],
        });

        drop(rx);

        push_manager.try_send(&value, &con_addr);
        push_manager.try_send(&value, &con_addr);
        push_manager.try_send(&value, &con_addr);
    }
    #[test]
    fn test_push_manager_without_sender() {
        let push_manager = PushManager::new();

        let con_addr = Arc::new("127.0.0.1:6379".to_string());
        let value = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Data("hello".to_string().into_bytes())],
        });

        push_manager.try_send(&value, &con_addr); // nothing happens!

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        push_manager.replace_sender(tx);
        push_manager.try_send(&value, &con_addr);

        assert_eq!(
            rx.try_recv().unwrap().data,
            vec![Value::Data("hello".to_string().into_bytes())]
        );
    }
    #[test]
    fn test_push_manager_multiple_channels_and_messages() {
        let push_manager = PushManager::new();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        push_manager.replace_sender(tx1);

        let con_addr1 = Arc::new("127.0.0.1:6379".to_string());
        let con_addr2 = Arc::new("127.0.0.1:6380".to_string());

        let value1 = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Int(1)],
        });

        let value2 = Ok(Value::Push {
            kind: PushKind::Message,
            data: vec![Value::Int(2)],
        });

        push_manager.try_send(&value1, &con_addr1);
        push_manager.try_send(&value2, &con_addr2);

        assert_eq!(rx1.try_recv().unwrap().data, vec![Value::Int(1)]);
        assert_eq!(rx1.try_recv().unwrap().data, vec![Value::Int(2)]);

        push_manager.replace_sender(tx2);
        push_manager.try_send(&value1, &con_addr2);
        push_manager.try_send(&value2, &con_addr1);

        assert_eq!(rx2.try_recv().unwrap().data, vec![Value::Int(1)]);
        assert_eq!(rx2.try_recv().unwrap().data, vec![Value::Int(2)]);
    }

    #[tokio::test]
    async fn test_push_manager_multi_threaded() {
        // In this test we create 4 channels and send 1000 message, it switchs channels for each message we sent.
        // Then we check if all messages are received and sum of messages are equal to expected sum.
        // We also check if all channels are used.
        let push_manager = PushManager::new();
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        let (tx3, mut rx3) = tokio::sync::mpsc::unbounded_channel();
        let (tx4, mut rx4) = tokio::sync::mpsc::unbounded_channel();

        let con_addr = Arc::new("127.0.0.1:6379".to_string());
        let mut handles = vec![];
        let txs = vec![tx1, tx2, tx3, tx4];
        let mut expected_sum = 0;
        for i in 0..1000 {
            expected_sum += i;
            let push_manager_clone = push_manager.clone();
            let con_addr = con_addr.clone();
            let new_tx = txs[(i % 4) as usize].clone();
            let value = Ok(Value::Push {
                kind: PushKind::Message,
                data: vec![Value::Int(i)],
            });
            let handle = tokio::spawn(async move {
                push_manager_clone.replace_sender(new_tx);
                push_manager_clone.try_send(&value, &con_addr);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut count1 = 0;
        let mut count2 = 0;
        let mut count3 = 0;
        let mut count4 = 0;
        let mut received_sum = 0;
        while let Ok(push_info) = rx1.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count1 += 1;
        }
        while let Ok(push_info) = rx2.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count2 += 1;
        }

        while let Ok(push_info) = rx3.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count3 += 1;
        }

        while let Ok(push_info) = rx4.try_recv() {
            assert_eq!(push_info.kind, PushKind::Message);
            if let Value::Int(i) = push_info.data[0] {
                received_sum += i;
            }
            count4 += 1;
        }

        assert_ne!(count1, 0);
        assert_ne!(count2, 0);
        assert_ne!(count3, 0);
        assert_ne!(count4, 0);

        assert_eq!(count1 + count2 + count3 + count4, 1000);
        assert_eq!(received_sum, expected_sum);
    }
}
