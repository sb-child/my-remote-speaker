use crate::aud::mixer::DeviceInfo;
use crossfire::{BlockingTxTrait, MRx, MTx, mpmc};
use dashmap::DashMap;
use std::{sync::Arc, time::Instant};
use tracing::debug;

/// 设备运行状态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceState {
    /// Audio host 已 spawn 或正在重启中，stream 还没完全准备好。
    Initializing,
    /// stream 已准备好。
    Ready,
    /// 设备从快照消失。如果持续消失会被清理。
    Disconnected,
    /// 设备断开超时已清理，或设备在黑名单里。
    Gone,
}

#[derive(Clone, Debug)]
pub struct DeviceEntry {
    pub info: DeviceInfo,
    pub state: DeviceState,
    /// 进入 Disconnected 的时刻
    pub disconnected_at: Option<Instant>,
}

/// 设备生命周期事件
#[derive(Clone, Debug)]
pub enum DeviceEvent {
    /// 设备首次出现。
    Added { info: DeviceInfo },
    /// stream 准备好。
    Ready { id: String },
    /// 设备从快照消失。
    Disconnected { id: String },
    /// 设备超时已回收，或设备在黑名单。
    Gone { id: String },
}

pub type DeviceEventTx = MTx<mpmc::Array<DeviceEvent>>;
pub type DeviceEventRx = MRx<mpmc::Array<DeviceEvent>>;

/// 设备目录。
#[derive(Clone)]
pub struct DeviceStates {
    inner: Arc<DashMap<String, DeviceEntry>>,
    events_tx: DeviceEventTx,
    events_rx: DeviceEventRx,
}

impl DeviceStates {
    pub(crate) fn new() -> Self {
        let (events_tx, events_rx) = crossfire::mpmc::bounded_blocking(32);
        Self {
            inner: Arc::new(DashMap::new()),
            events_tx,
            events_rx,
        }
    }

    /// 订阅设备生命周期事件。
    pub fn events(&self) -> DeviceEventRx {
        self.events_rx.clone()
    }

    pub fn get(&self, id: &str) -> Option<DeviceEntry> {
        self.inner.get(id).map(|e| e.clone())
    }

    /// 当前设备目录快照。
    /// - id -> entry。
    pub fn snapshot(&self) -> Vec<(String, DeviceEntry)> {
        self.inner
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }

    /// 记录设备出现事件。
    pub(crate) fn add(&self, info: DeviceInfo) {
        if self.inner.contains_key(&info.id) {
            self.transition(&info.id, DeviceState::Initializing);
            return;
        }
        self.inner.insert(
            info.id.clone(),
            DeviceEntry {
                state: DeviceState::Initializing,
                disconnected_at: None,
                info: info.clone(),
            },
        );
        self.send(DeviceEvent::Added { info });
    }

    /// 状态迁移，返回是否发生变化。
    pub(crate) fn transition(&self, id: &str, next: DeviceState) -> bool {
        let mut changed = false;
        if let Some(mut e) = self.inner.get_mut(id) {
            if e.state != next {
                e.state = next;
                e.disconnected_at = if next == DeviceState::Disconnected {
                    Some(Instant::now())
                } else {
                    None
                };
                changed = true;
            }
        }
        if changed {
            match next {
                DeviceState::Initializing => {}
                DeviceState::Ready => self.send(DeviceEvent::Ready { id: id.to_owned() }),
                DeviceState::Disconnected => {
                    self.send(DeviceEvent::Disconnected { id: id.to_owned() })
                }
                DeviceState::Gone => self.send(DeviceEvent::Gone { id: id.to_owned() }),
            }
        }
        changed
    }

    /// 移除设备，广播 Gone。
    pub(crate) fn remove(&self, id: &str) {
        if self.inner.remove(id).is_some() {
            self.send(DeviceEvent::Gone { id: id.to_owned() });
        }
    }

    fn send(&self, ev: DeviceEvent) {
        if self.events_tx.try_send(ev).is_err() {
            debug!("device event channel full, event dropped");
        }
    }
}

impl Default for DeviceStates {
    fn default() -> Self {
        Self::new()
    }
}
