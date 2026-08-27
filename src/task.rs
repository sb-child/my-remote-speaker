use dashmap::DashMap;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type TaskId = u64;

#[derive(Clone)]
pub enum TaskState {
    Pending,
    Running(Arc<dyn Any + Send + Sync>),
    Completed(Arc<dyn Any + Send + Sync>),
    Failed(Arc<dyn Any + Send + Sync>),
}

impl TaskState {
    pub fn downcast_running<P: 'static + Sync + Send>(&self) -> Option<Arc<P>> {
        if let TaskState::Running(val) = self {
            val.clone().downcast::<P>().ok()
        } else {
            None
        }
    }

    pub fn downcast_completed<T: 'static + Sync + Send>(&self) -> Option<Arc<T>> {
        if let TaskState::Completed(val) = self {
            val.clone().downcast::<T>().ok()
        } else {
            None
        }
    }

    pub fn downcast_failed<E: 'static + Sync + Send>(&self) -> Option<Arc<E>> {
        if let TaskState::Failed(val) = self {
            val.clone().downcast::<E>().ok()
        } else {
            None
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Completed(_) | TaskState::Failed(_))
    }
}

#[derive(Clone, Default)]
pub struct TaskManager {
    next_id: Arc<AtomicU64>,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: Arc<DashMap<TaskId, JoinHandle<()>>>,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
            tasks: Arc::new(DashMap::new()),
            handles: Arc::new(DashMap::new()),
        }
    }

    pub fn spawn<F, Fut, P, T, E>(&self, f: F) -> TaskId
    where
        F: FnOnce(mpsc::UnboundedSender<P>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
        P: Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let task_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (status_tx, mut status_rx) = mpsc::unbounded_channel::<P>();
        self.tasks.insert(task_id, TaskState::Pending);
        let tasks_for_progress = Arc::clone(&self.tasks);
        let tasks_for_result = Arc::clone(&self.tasks);
        let handles_for_cleanup = Arc::clone(&self.handles);
        let progress_handle = tokio::spawn(async move {
            while let Some(progress) = status_rx.recv().await {
                tasks_for_progress.insert(task_id, TaskState::Running(Arc::new(progress)));
            }
        });
        let worker_handle = tokio::spawn(async move {
            let result = f(status_tx).await;
            progress_handle.abort();
            match result {
                Ok(val) => {
                    tasks_for_result.insert(task_id, TaskState::Completed(Arc::new(val)));
                }
                Err(err) => {
                    tasks_for_result.insert(task_id, TaskState::Failed(Arc::new(err)));
                }
            }
            handles_for_cleanup.remove(&task_id);
            let tasks_for_ttl = Arc::clone(&tasks_for_result);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks_for_ttl.remove(&task_id);
            });
        });
        self.handles.insert(task_id, worker_handle);
        task_id
    }

    pub fn get_status(&self, task_id: TaskId) -> Option<TaskState> {
        let state = self.tasks.get(&task_id)?.value().clone();
        if state.is_terminal() {
            self.tasks.remove(&task_id);
            self.handles.remove(&task_id);
        }
        Some(state)
    }

    pub fn remove_task(&self, task_id: TaskId) {
        if let Some((_, handle)) = self.handles.remove(&task_id) {
            handle.abort();
        }
        self.tasks.remove(&task_id);
    }

    pub fn close(&self) {
        let task_ids: Vec<TaskId> = self.handles.iter().map(|entry| *entry.key()).collect();
        for id in task_ids {
            if let Some((_, handle)) = self.handles.remove(&id) {
                handle.abort();
            }
            self.tasks.remove(&id);
        }
    }
}
