use dashmap::DashMap;
use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tarpc::tokio_util::sync::CancellationToken;
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

pub struct TaskChannel<P>
where
    P: Send + Sync + 'static + Unpin,
{
    mtx: crossfire::MTx<crossfire::mpsc::Array<P>>,
    matx: crossfire::MAsyncTx<crossfire::mpsc::Array<P>>,
}

impl<P> TaskChannel<P>
where
    P: Send + Sync + 'static + Unpin,
{
    pub fn new() -> (Self, crossfire::AsyncRx<crossfire::mpsc::Array<P>>) {
        let (matx, rx) = crossfire::mpsc::bounded_async::<P>(16);
        let mtx = matx.clone().into_blocking();
        (Self { matx, mtx }, rx)
    }

    pub fn update(&self, state: P) -> Result<(), crossfire::SendError<P>> {
        self.mtx.send(state)
    }

    pub async fn update_async(&self, state: P) -> Result<(), crossfire::SendError<P>> {
        self.matx.send(state).await
    }
}

#[derive(Clone, Default)]
pub struct TaskManager {
    next_id: Arc<AtomicU64>,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: Arc<DashMap<TaskId, (JoinHandle<()>, CancellationToken)>>,
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
        F: FnOnce(TaskChannel<P>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let task_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let token = CancellationToken::new();
        let task_token = token.clone();
        let (status_tx, status_rx) = TaskChannel::new();
        drop(self.tasks.insert(task_id, TaskState::Pending));
        let tasks_for_progress = Arc::clone(&self.tasks);
        let tasks_for_result = Arc::clone(&self.tasks);
        let handles_for_cleanup = Arc::clone(&self.handles);
        let progress_handle = tokio::spawn(async move {
            while let Ok(progress) = status_rx.recv().await {
                tasks_for_progress.insert(task_id, TaskState::Running(Arc::new(progress)));
            }
        });
        let worker_handle = tokio::spawn(async move {
            let result = f(status_tx, task_token).await;
            progress_handle.abort();
            match result {
                Ok(val) => {
                    tasks_for_result.insert(task_id, TaskState::Completed(Arc::new(val)));
                }
                Err(err) => {
                    tasks_for_result.insert(task_id, TaskState::Failed(Arc::new(err)));
                }
            }
            drop(handles_for_cleanup.remove(&task_id));
            let tasks_for_ttl = Arc::clone(&tasks_for_result);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks_for_ttl.remove(&task_id);
            });
        });
        self.handles.insert(task_id, (worker_handle, token));
        task_id
    }

    pub fn spawn_blocking<F, P, T, E>(&self, f: F) -> TaskId
    where
        F: FnOnce(TaskChannel<P>, CancellationToken) -> Result<T, E> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let task_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let token = CancellationToken::new();
        let task_token = token.clone();
        let (status_tx, status_rx) = TaskChannel::new();
        drop(self.tasks.insert(task_id, TaskState::Pending));
        let tasks_for_progress = Arc::clone(&self.tasks);
        let tasks_for_result = Arc::clone(&self.tasks);
        let handles_for_cleanup = Arc::clone(&self.handles);
        let progress_handle = tokio::spawn(async move {
            while let Ok(progress) = status_rx.recv().await {
                tasks_for_progress.insert(task_id, TaskState::Running(Arc::new(progress)));
            }
        });
        let worker_handle = tokio::spawn(async move {
            let blocking_res = tokio::task::spawn_blocking(move || f(status_tx, task_token)).await;
            progress_handle.abort();
            match blocking_res {
                Ok(Ok(val)) => {
                    tasks_for_result.insert(task_id, TaskState::Completed(Arc::new(val)));
                }
                Ok(Err(err)) => {
                    tasks_for_result.insert(task_id, TaskState::Failed(Arc::new(err)));
                }
                Err(_err) => {
                    // JoinError
                    handles_for_cleanup.remove(&task_id);
                    return;
                }
            }
            drop(handles_for_cleanup.remove(&task_id));
            let tasks_for_ttl = Arc::clone(&tasks_for_result);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks_for_ttl.remove(&task_id);
            });
        });
        self.handles.insert(task_id, (worker_handle, token));
        task_id
    }

    pub fn get_status(&self, task_id: TaskId) -> Option<TaskState> {
        let state = self.tasks.get(&task_id)?.value().clone();
        if state.is_terminal() {
            self.handles.remove(&task_id);
        }
        Some(state)
    }

    pub fn cancel_task(&self, task_id: TaskId) {
        if let Some((_, (handle, token))) = self.handles.remove(&task_id) {
            token.cancel();
            handle.abort();
        }
    }

    pub fn remove_task(&self, task_id: TaskId) {
        self.cancel_task(task_id);
        self.tasks.remove(&task_id);
    }

    pub fn close(&self) {
        let task_ids: Vec<TaskId> = self.handles.iter().map(|entry| *entry.key()).collect();
        for id in task_ids {
            self.cancel_task(id);
            self.tasks.remove(&id);
        }
    }
}
