use dashmap::DashMap;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    fmt,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskId(u64);

struct AtomicTaskId(AtomicU64);

impl Default for AtomicTaskId {
    fn default() -> Self {
        Self(AtomicU64::new(1))
    }
}

impl AtomicTaskId {
    fn next(&self) -> TaskId {
        TaskId(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "Task panicked with unknown payload".to_string()
    }
}

#[derive(Clone)]
pub enum TaskState {
    Pending,
    Running(Arc<dyn Any + Send + Sync>),
    Cancelling,
    Completed(Arc<dyn Any + Send + Sync>),
    Failed(Arc<dyn Any + Send + Sync>),
    Cancelled,
    Panicked(Arc<String>),
}

impl TaskState {
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running(_))
    }
    pub fn is_cancelling(&self) -> bool {
        matches!(self, Self::Cancelling)
    }
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
    pub fn is_panicked(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed(_) | Self::Failed(_) | Self::Cancelled | Self::Panicked(_)
        )
    }

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

    pub fn panicked_message(&self) -> Option<Arc<String>> {
        if let TaskState::Panicked(msg) = self {
            Some(msg.clone())
        } else {
            None
        }
    }

    pub fn into_result<T, E>(&self) -> Option<Result<Arc<T>, TaskError<E>>>
    where
        T: 'static + Send + Sync,
        E: 'static + Send + Sync,
    {
        match self {
            TaskState::Completed(val) => val.clone().downcast::<T>().ok().map(Ok),
            TaskState::Failed(err) => err
                .clone()
                .downcast::<E>()
                .ok()
                .map(|e| Err(TaskError::Failed(e))),
            TaskState::Cancelled => Some(Err(TaskError::Cancelled)),
            TaskState::Panicked(msg) => Some(Err(TaskError::Panicked(msg.clone()))),
            _ => None,
        }
    }

    pub fn to_typed<P, T, E>(&self) -> Option<TypedTaskState<P, T, E>>
    where
        P: 'static + Send + Sync,
        T: 'static + Send + Sync,
        E: 'static + Send + Sync,
    {
        Some(match self {
            TaskState::Pending => TypedTaskState::Pending,
            TaskState::Cancelling => TypedTaskState::Cancelling,
            TaskState::Cancelled => TypedTaskState::Cancelled,
            TaskState::Panicked(msg) => TypedTaskState::Panicked(msg.clone()),
            TaskState::Running(v) => v
                .clone()
                .downcast::<P>()
                .map(TypedTaskState::Running)
                .ok()?,
            TaskState::Completed(v) => v
                .clone()
                .downcast::<T>()
                .map(TypedTaskState::Completed)
                .ok()?,
            TaskState::Failed(v) => v.clone().downcast::<E>().map(TypedTaskState::Failed).ok()?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum TaskError<E> {
    Failed(Arc<E>),
    Cancelled,
    Panicked(Arc<String>),
}

impl fmt::Debug for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Pending => write!(f, "TaskState::Pending"),
            TaskState::Running(_) => write!(f, "TaskState::Running"),
            TaskState::Cancelling => write!(f, "TaskState::Cancelling"),
            TaskState::Completed(_) => write!(f, "TaskState::Completed"),
            TaskState::Failed(_) => write!(f, "TaskState::Failed"),
            TaskState::Cancelled => write!(f, "TaskState::Cancelled"),
            TaskState::Panicked(_) => write!(f, "TaskState::Panicked"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum TypedTaskState<P, T, E> {
    Pending,
    Running(Arc<P>),
    Cancelling,
    Completed(Arc<T>),
    Failed(Arc<E>),
    Cancelled,
    Panicked(Arc<String>),
    Invalid,
}

impl<P, T, E> TypedTaskState<P, T, E> {
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running(_))
    }
    pub fn is_cancelling(&self) -> bool {
        matches!(self, Self::Cancelling)
    }
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed(_))
    }
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
    pub fn is_panicked(&self) -> bool {
        matches!(self, Self::Panicked(_))
    }
    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid)
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed(_)
                | Self::Failed(_)
                | Self::Cancelled
                | Self::Panicked(_)
                | Self::Invalid
        )
    }
}

pub struct ProgressChannel<P>
where
    P: Send + Sync + 'static + Unpin,
{
    mtx: crossfire::MTx<crossfire::mpsc::Array<P>>,
    matx: crossfire::MAsyncTx<crossfire::mpsc::Array<P>>,
}

impl<P> ProgressChannel<P>
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

type HandleMap = Arc<
    DashMap<
        TaskId,
        (
            JoinHandle<()>,
            CancellationToken,
            crossfire::MAsyncRx<crossfire::mpmc::Null>,
        ),
    >,
>;

struct TaskGuard {
    task_id: TaskId,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: HandleMap,
    progress_handle: Option<JoinHandle<()>>,
    ttl: Duration,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if let Some(ph) = self.progress_handle.take() {
            ph.abort();
        }
        self.handles.remove(&self.task_id);
        if let Some(current_state) = self.tasks.get(&self.task_id) {
            let is_handled = current_state.is_terminal() || current_state.is_cancelling();
            let is_cancelling = current_state.is_cancelling();
            drop(current_state);
            if !is_handled {
                self.tasks.insert(
                    self.task_id,
                    TaskState::Panicked(Arc::new(
                        "Task executed with panic or aborted".to_string(),
                    )),
                );
            }
            if !is_cancelling {
                let tasks = Arc::clone(&self.tasks);
                let task_id = self.task_id;
                let ttl = self.ttl;
                tokio::spawn(async move {
                    tokio::time::sleep(ttl).await;
                    tasks.remove(&task_id);
                });
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct TaskManager {
    task_id_counter: Arc<AtomicTaskId>,
    closed: Arc<AtomicBool>,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: HandleMap,
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            task_id_counter: Arc::new(AtomicTaskId::default()),
            closed: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(DashMap::new()),
            handles: Arc::new(DashMap::new()),
        }
    }

    pub fn spawn_typed<F, Fut, P, T, E>(&self, f: F) -> TaskHandle<P, T, E>
    where
        F: FnOnce(ProgressChannel<P>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let id = self.spawn(f);
        TaskHandle::new(id, self.clone())
    }

    pub fn spawn_blocking_typed<F, P, T, E>(&self, f: F) -> TaskHandle<P, T, E>
    where
        F: FnOnce(ProgressChannel<P>, CancellationToken) -> Result<T, E> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let id = self.spawn_blocking(f);
        TaskHandle::new(id, self.clone())
    }

    pub fn spawn<F, Fut, P, T, E>(&self, f: F) -> TaskId
    where
        F: FnOnce(ProgressChannel<P>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, E>> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let task_id = self.task_id_counter.next();
        if self.closed.load(Ordering::SeqCst) {
            self.tasks.insert(task_id, TaskState::Cancelled);
            let tasks = Arc::clone(&self.tasks);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks.remove(&task_id);
            });
            return task_id;
        }
        let token = CancellationToken::new();
        let task_token = token.clone();
        let (status_tx, status_rx) = ProgressChannel::new();
        let (death_tx, death_rx) = crossfire::mpmc::Null::new().new_async();
        self.tasks.insert(task_id, TaskState::Pending);
        let tasks_for_progress = Arc::clone(&self.tasks);
        let tasks_for_result = Arc::clone(&self.tasks);
        let handles_ref = Arc::clone(&self.handles);
        let progress_handle = tokio::spawn(async move {
            while let Ok(progress) = status_rx.recv().await {
                tasks_for_progress.insert(task_id, TaskState::Running(Arc::new(progress)));
            }
        });
        let worker_handle = tokio::spawn(async move {
            let mut guard = TaskGuard {
                task_id,
                tasks: Arc::clone(&tasks_for_result),
                handles: handles_ref,
                progress_handle: Some(progress_handle),
                ttl: Duration::from_secs(60),
            };
            let _death_tx = death_tx;
            let res =
                FutureExt::catch_unwind(AssertUnwindSafe(async { f(status_tx, task_token).await }))
                    .await;
            let is_cancelling = tasks_for_result
                .get(&task_id)
                .map(|s| s.is_cancelling() || s.is_cancelled())
                .unwrap_or(false);
            if !is_cancelling {
                match res {
                    Ok(Ok(val)) => {
                        tasks_for_result.insert(task_id, TaskState::Completed(Arc::new(val)));
                    }
                    Ok(Err(err)) => {
                        tasks_for_result.insert(task_id, TaskState::Failed(Arc::new(err)));
                    }
                    Err(panic) => {
                        let msg = panic_payload_to_string(panic.as_ref());
                        tasks_for_result.insert(task_id, TaskState::Panicked(Arc::new(msg)));
                    }
                }
            }
            if let Some(ph) = guard.progress_handle.take() {
                ph.abort();
            }
        });
        self.handles
            .insert(task_id, (worker_handle, token, death_rx));
        if self.closed.load(Ordering::SeqCst) {
            self.cancel_task(task_id);
        } else if let Some(state) = self.tasks.get(&task_id) {
            if state.is_terminal() {
                self.handles.remove(&task_id);
            }
        }
        task_id
    }

    pub fn spawn_blocking<F, P, T, E>(&self, f: F) -> TaskId
    where
        F: FnOnce(ProgressChannel<P>, CancellationToken) -> Result<T, E> + Send + 'static,
        P: Send + Sync + 'static + Unpin,
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let task_id = self.task_id_counter.next();
        if self.closed.load(Ordering::SeqCst) {
            self.tasks.insert(task_id, TaskState::Cancelled);
            let tasks = Arc::clone(&self.tasks);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks.remove(&task_id);
            });
            return task_id;
        }
        let token = CancellationToken::new();
        let task_token = token.clone();
        let (status_tx, status_rx) = ProgressChannel::new();
        let (death_tx, death_rx) = crossfire::mpmc::Null::new().new_async();
        self.tasks.insert(task_id, TaskState::Pending);
        let tasks_for_progress = Arc::clone(&self.tasks);
        let tasks_for_result = Arc::clone(&self.tasks);
        let handles_ref = Arc::clone(&self.handles);
        let progress_handle = tokio::spawn(async move {
            while let Ok(progress) = status_rx.recv().await {
                tasks_for_progress.insert(task_id, TaskState::Running(Arc::new(progress)));
            }
        });
        let worker_handle = tokio::spawn(async move {
            let mut guard = TaskGuard {
                task_id,
                tasks: Arc::clone(&tasks_for_result),
                handles: handles_ref,
                progress_handle: Some(progress_handle),
                ttl: Duration::from_secs(60),
            };
            let blocking_res = tokio::task::spawn_blocking(move || {
                let _death_tx = death_tx;
                f(status_tx, task_token)
            })
            .await;
            let is_cancelling = tasks_for_result
                .get(&task_id)
                .map(|s| s.is_cancelling() || s.is_cancelled())
                .unwrap_or(false);
            if !is_cancelling {
                match blocking_res {
                    Ok(Ok(val)) => {
                        tasks_for_result.insert(task_id, TaskState::Completed(Arc::new(val)));
                    }
                    Ok(Err(err)) => {
                        tasks_for_result.insert(task_id, TaskState::Failed(Arc::new(err)));
                    }
                    Err(join_err) => {
                        let msg = match join_err.try_into_panic() {
                            Ok(panic_err) => panic_payload_to_string(panic_err.as_ref()),
                            Err(_join_err) => "Task was cancelled or aborted".to_string(),
                        };
                        tasks_for_result.insert(task_id, TaskState::Panicked(Arc::new(msg)));
                    }
                }
            }
            if let Some(ph) = guard.progress_handle.take() {
                ph.abort();
            }
        });
        self.handles
            .insert(task_id, (worker_handle, token, death_rx));
        if self.closed.load(Ordering::SeqCst) {
            self.cancel_task(task_id);
        } else if let Some(state) = self.tasks.get(&task_id) {
            if state.is_terminal() {
                self.handles.remove(&task_id);
            }
        }
        task_id
    }

    /// 获取任务当前状态。
    pub fn get_status(&self, task_id: TaskId) -> Option<TaskState> {
        let state = self.tasks.get(&task_id)?.value().clone();
        if state.is_terminal() {
            self.handles.remove(&task_id);
        }
        Some(state)
    }

    /// 立刻取消任务。
    /// - 设置 `state = TaskState::Cancelling` 并触发任务的 `CancellationToken` 然后等待 5 秒。
    /// - 如果任务仍未关闭则调用 `handle.abort()`。
    /// - 最后等待任务彻底关闭后设置 `state = TaskState::Cancelled`。
    pub fn cancel_task(&self, task_id: TaskId) {
        if let Some(mut state) = self.tasks.get_mut(&task_id) {
            if state.is_terminal() || matches!(*state, TaskState::Cancelling) {
                return;
            }
            *state = TaskState::Cancelling;
        } else {
            return;
        }
        if let Some((_, (handle, token, death_rx))) = self.handles.remove(&task_id) {
            token.cancel();
            let tasks = Arc::clone(&self.tasks);
            tokio::spawn(async move {
                let graceful_exit =
                    tokio::time::timeout(Duration::from_secs(5), &mut death_rx.recv()).await;
                if graceful_exit.is_err() {
                    handle.abort(); // async task only
                    // 如果task还没死就一直等着
                    let _ = death_rx.recv().await;
                    let _ = handle.await;
                }
                if let Some(mut state) = tasks.get_mut(&task_id) {
                    *state = TaskState::Cancelled;
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks.remove(&task_id);
            });
        }
    }

    /// 注册触发器，在 ct 触发时取消任务。
    pub fn cancel_task_at(&self, task_id: TaskId, ct: &CancellationToken) {
        let tm = self.clone();
        let ct = ct.clone();
        tokio::spawn(async move {
            loop {
                let check = tokio::time::timeout(Duration::from_millis(500), ct.cancelled()).await;
                if let Err(_) = check {
                    if tm.is_closed() {
                        break;
                    } else if tm
                        .get_status(task_id)
                        .map(|s| s.is_terminal())
                        .unwrap_or(true)
                    {
                        break;
                    }
                } else {
                    tm.cancel_task(task_id);
                    break;
                }
            }
        });
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        let task_ids: Vec<TaskId> = self.handles.iter().map(|entry| *entry.key()).collect();
        for id in task_ids {
            self.cancel_task(id);
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

pub struct TaskHandle<P, T, E> {
    pub id: TaskId,
    tm: TaskManager,
    _phantom: PhantomData<fn() -> (P, T, E)>,
}

impl<P, T, E> TaskHandle<P, T, E>
where
    P: 'static + Send + Sync,
    T: 'static + Send + Sync,
    E: 'static + Send + Sync,
{
    pub fn new(id: TaskId, tm: TaskManager) -> Self {
        Self {
            id,
            tm,
            _phantom: PhantomData,
        }
    }

    /// 获取任务当前状态。
    pub fn status(&self) -> TypedTaskState<P, T, E> {
        self.tm
            .get_status(self.id)
            .map(|s| s.to_typed())
            .flatten()
            .unwrap_or(TypedTaskState::Invalid)
    }

    /// 立刻取消任务。
    pub fn cancel(&self) {
        self.tm.cancel_task(self.id);
    }

    /// 注册触发器，在 ct 触发时取消任务。
    pub fn cancel_at(&self, ct: &CancellationToken) {
        self.tm.cancel_task_at(self.id, ct);
    }
}
