use crate::use_id;
use dashmap::DashMap;
use futures_util::FutureExt;
use std::{
    any::Any,
    fmt,
    marker::PhantomData,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use_id!(Task);

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

    pub fn downcast_running<Progress: 'static + Sync + Send>(&self) -> Option<Arc<Progress>> {
        if let TaskState::Running(val) = self {
            val.clone().downcast::<Progress>().ok()
        } else {
            None
        }
    }

    pub fn downcast_completed<Ret: 'static + Sync + Send>(&self) -> Option<Arc<Ret>> {
        if let TaskState::Completed(val) = self {
            val.clone().downcast::<Ret>().ok()
        } else {
            None
        }
    }

    pub fn downcast_failed<Err: 'static + Sync + Send>(&self) -> Option<Arc<Err>> {
        if let TaskState::Failed(val) = self {
            val.clone().downcast::<Err>().ok()
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

    pub fn into_result<Ret, Err>(&self) -> Option<Result<Arc<Ret>, TaskError<Err>>>
    where
        Ret: 'static + Send + Sync,
        Err: 'static + Send + Sync,
    {
        match self {
            TaskState::Completed(val) => val.clone().downcast::<Ret>().ok().map(Ok),
            TaskState::Failed(err) => err
                .clone()
                .downcast::<Err>()
                .ok()
                .map(|e| Err(TaskError::Failed(e))),
            TaskState::Cancelled => Some(Err(TaskError::Cancelled)),
            TaskState::Panicked(msg) => Some(Err(TaskError::Panicked(msg.clone()))),
            _ => None,
        }
    }

    pub fn to_typed<Status, Ret, Err>(&self) -> TypedTaskState<Status, Ret, Err>
    where
        Status: 'static + Send + Sync,
        Ret: 'static + Send + Sync,
        Err: 'static + Send + Sync,
    {
        match self {
            TaskState::Pending => TypedTaskState::Pending,
            TaskState::Cancelling => TypedTaskState::Cancelling,
            TaskState::Cancelled => TypedTaskState::Cancelled,
            TaskState::Panicked(msg) => TypedTaskState::Panicked(msg.clone()),
            TaskState::Running(v) => v
                .clone()
                .downcast::<Status>()
                .map(TypedTaskState::Running)
                .unwrap_or(TypedTaskState::Invalid),
            TaskState::Completed(v) => v
                .clone()
                .downcast::<Ret>()
                .map(TypedTaskState::Completed)
                .unwrap_or(TypedTaskState::Invalid),
            TaskState::Failed(v) => v
                .clone()
                .downcast::<Err>()
                .map(TypedTaskState::Failed)
                .unwrap_or(TypedTaskState::Invalid),
        }
    }

    pub fn as_typed<'a, Status, Ret, Err>(&'a self) -> TypedTaskStateRef<'a, Status, Ret, Err>
    where
        Status: 'static + Send + Sync,
        Ret: 'static + Send + Sync,
        Err: 'static + Send + Sync,
    {
        match self {
            TaskState::Pending => TypedTaskStateRef::Pending,
            TaskState::Cancelling => TypedTaskStateRef::Cancelling,
            TaskState::Cancelled => TypedTaskStateRef::Cancelled,
            TaskState::Panicked(msg) => TypedTaskStateRef::Panicked(msg),
            TaskState::Running(v) => v
                .downcast_ref::<Status>()
                .map(TypedTaskStateRef::Running)
                .unwrap_or(TypedTaskStateRef::Invalid),
            TaskState::Completed(v) => v
                .downcast_ref::<Ret>()
                .map(TypedTaskStateRef::Completed)
                .unwrap_or(TypedTaskStateRef::Invalid),
            TaskState::Failed(v) => v
                .downcast_ref::<Err>()
                .map(TypedTaskStateRef::Failed)
                .unwrap_or(TypedTaskStateRef::Invalid),
        }
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

pub enum TypedTaskStateRef<'a, Status, Ret, Err> {
    /// 任务刚刚启动，还没有汇报状态。
    Pending,
    /// 任务正在运行，并汇报了当前状态。
    Running(&'a Status),
    /// 任务正在取消。
    Cancelling,
    /// 任务已经完成。
    Completed(&'a Ret),
    /// 任务已经失败。
    Failed(&'a Err),
    /// 任务已经取消。
    Cancelled,
    /// 任务已经崩溃。
    Panicked(&'a String),
    /// 类型cast失败，或任务不存在。
    Invalid,
}

impl<'a, Status, Ret, Err> TypedTaskStateRef<'a, Status, Ret, Err> {
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

    /// 任务是否已进入终态。
    /// - 注意类型 cast 失败会导致任务为 Invalid 状态，仍然会被当做进入终态。
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

#[derive(Clone, Debug)]
pub enum TypedTaskState<Status, Ret, Err> {
    /// 任务刚刚启动，还没有汇报状态。
    Pending,
    /// 任务正在运行，并汇报了当前状态。
    Running(Arc<Status>),
    /// 任务正在取消。
    Cancelling,
    /// 任务已经完成。
    Completed(Arc<Ret>),
    /// 任务已经失败。
    Failed(Arc<Err>),
    /// 任务已经取消。
    Cancelled,
    /// 任务已经崩溃。
    Panicked(Arc<String>),
    /// 类型cast失败，或任务不存在。
    Invalid,
}

impl<'a, Status, Ret, Err> From<&'a TypedTaskState<Status, Ret, Err>>
    for TypedTaskStateRef<'a, Status, Ret, Err>
{
    fn from(state: &'a TypedTaskState<Status, Ret, Err>) -> Self {
        match state {
            TypedTaskState::Pending => Self::Pending,
            TypedTaskState::Cancelling => Self::Cancelling,
            TypedTaskState::Cancelled => Self::Cancelled,
            TypedTaskState::Panicked(msg) => Self::Panicked(msg),
            TypedTaskState::Running(v) => Self::Running(v.as_ref()),
            TypedTaskState::Completed(v) => Self::Completed(v.as_ref()),
            TypedTaskState::Failed(v) => Self::Failed(v.as_ref()),
            TypedTaskState::Invalid => Self::Invalid,
        }
    }
}

impl<'a, Status: Clone, Ret: Clone, Err: Clone> From<TypedTaskStateRef<'a, Status, Ret, Err>>
    for TypedTaskState<Status, Ret, Err>
{
    fn from(state_ref: TypedTaskStateRef<'a, Status, Ret, Err>) -> Self {
        match state_ref {
            TypedTaskStateRef::Pending => Self::Pending,
            TypedTaskStateRef::Cancelling => Self::Cancelling,
            TypedTaskStateRef::Cancelled => Self::Cancelled,
            TypedTaskStateRef::Panicked(msg) => Self::Panicked(Arc::new(msg.clone())),
            TypedTaskStateRef::Running(v) => Self::Running(Arc::new(v.clone())),
            TypedTaskStateRef::Completed(v) => Self::Completed(Arc::new(v.clone())),
            TypedTaskStateRef::Failed(v) => Self::Failed(Arc::new(v.clone())),
            TypedTaskStateRef::Invalid => Self::Invalid,
        }
    }
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

    /// 任务是否已进入终态。
    /// - 注意类型 cast 失败会导致任务为 Invalid 状态，仍然会被当做进入终态。
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

pub struct ProgressUpdater<Status>
where
    Status: Send + Sync + 'static + Unpin,
{
    tasks: Arc<DashMap<TaskId, TaskState>>,
    changes: watch::Sender<u64>,
    task_id: TaskId,
    _phantom: PhantomData<Status>,
}

impl<Status> ProgressUpdater<Status>
where
    Status: Send + Sync + 'static + Unpin,
{
    pub fn update(&self, state: Status) {
        let mut transitioned = false;
        self.tasks.alter(&self.task_id, |_k, v| {
            match &v {
                TaskState::Pending => transitioned = true,
                TaskState::Running(_) => {}
                _ => return v,
            }
            TaskState::Running(Arc::new(state))
        });
        if transitioned {
            let _ = self.changes.send_modify(|e| *e += 1);
        }
    }
}

type HandleMap = Arc<
    DashMap<
        TaskId,
        (
            JoinHandle<()>,
            CancellationToken,
            Option<crossfire::MAsyncRx<crossfire::mpmc::Null>>, // blocking only
        ),
    >,
>;

struct TaskGuard {
    task_id: TaskId,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: HandleMap,
    changes: watch::Sender<u64>,
    ttl: Duration,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
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
                let tasks = self.tasks.clone();
                let task_id = self.task_id;
                let ttl = self.ttl;
                tokio::spawn(async move {
                    tokio::time::sleep(ttl).await;
                    tasks.remove(&task_id);
                });
            }
        }
        // 在任务实体已退出时，唤醒 wait_for/wait_terminal。
        let _ = self.changes.send_modify(|e| *e += 1);
    }
}

#[derive(Clone)]
pub struct TaskManager {
    task_id_counter: Arc<TaskIdCounter>,
    closed: Arc<AtomicBool>,
    tasks: Arc<DashMap<TaskId, TaskState>>,
    handles: HandleMap,
    changes: watch::Sender<u64>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    pub fn new() -> Self {
        Self {
            task_id_counter: Arc::new(TaskIdCounter::default()),
            closed: Arc::new(AtomicBool::new(false)),
            tasks: Arc::new(DashMap::new()),
            handles: Arc::new(DashMap::new()),
            changes: watch::channel(0).0,
        }
    }

    pub fn spawn_typed<F, Fut, Status, Ret, Err>(&self, f: F) -> TaskHandle<Status, Ret, Err>
    where
        F: FnOnce(ProgressUpdater<Status>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Ret, Err>> + Send + 'static,
        Status: Send + Sync + 'static + Unpin,
        Ret: Send + Sync + 'static,
        Err: Send + Sync + 'static,
    {
        let id = self.spawn(f);
        TaskHandle::new(id, self.clone())
    }

    pub fn spawn_blocking_typed<F, Status, Ret, Err>(&self, f: F) -> TaskHandle<Status, Ret, Err>
    where
        F: FnOnce(ProgressUpdater<Status>, CancellationToken) -> Result<Ret, Err> + Send + 'static,
        Status: Send + Sync + 'static + Unpin,
        Ret: Send + Sync + 'static,
        Err: Send + Sync + 'static,
    {
        let id = self.spawn_blocking(f);
        TaskHandle::new(id, self.clone())
    }

    pub fn spawn<F, Fut, Status, Ret, Err>(&self, f: F) -> TaskId
    where
        F: FnOnce(ProgressUpdater<Status>, CancellationToken) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Ret, Err>> + Send + 'static,
        Status: Send + Sync + 'static + Unpin,
        Ret: Send + Sync + 'static,
        Err: Send + Sync + 'static,
    {
        let task_id = self.task_id_counter.next();
        if self.closed.load(Ordering::SeqCst) {
            self.tasks.insert(task_id, TaskState::Cancelled);
            self.notify_change();
            let tasks = self.tasks.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks.remove(&task_id);
            });
            return task_id;
        }
        let token = CancellationToken::new();
        let task_token = token.clone();
        let progress = ProgressUpdater {
            tasks: self.tasks.clone(),
            changes: self.changes.clone(),
            task_id,
            _phantom: PhantomData,
        };
        self.tasks.insert(task_id, TaskState::Pending);
        let tasks_for_result = self.tasks.clone();
        let handles_ref = self.handles.clone();
        let changes = self.changes.clone();
        let worker_handle = tokio::spawn(async move {
            let _guard = TaskGuard {
                task_id,
                tasks: tasks_for_result.clone(),
                handles: handles_ref,
                changes,
                ttl: Duration::from_secs(60),
            };
            let res =
                FutureExt::catch_unwind(AssertUnwindSafe(async { f(progress, task_token).await }))
                    .await;
            let terminal_state = match res {
                Ok(Ok(val)) => TaskState::Completed(Arc::new(val)),
                Ok(Err(err)) => TaskState::Failed(Arc::new(err)),
                Err(panic) => {
                    let msg = panic_payload_to_string(panic.as_ref());
                    TaskState::Panicked(Arc::new(msg))
                }
            };
            tasks_for_result.alter(&task_id, |_k, v| {
                match v.is_cancelling() || v.is_cancelled() {
                    true => v,
                    false => terminal_state,
                }
            });
        });
        self.handles.insert(task_id, (worker_handle, token, None));
        if self.closed.load(Ordering::SeqCst) {
            self.cancel_task(task_id);
        } else if let Some(state) = self.tasks.get(&task_id) {
            if state.is_terminal() {
                self.handles.remove(&task_id);
            }
        }
        task_id
    }

    pub fn spawn_blocking<F, Status, Ret, Err>(&self, f: F) -> TaskId
    where
        F: FnOnce(ProgressUpdater<Status>, CancellationToken) -> Result<Ret, Err> + Send + 'static,
        Status: Send + Sync + 'static + Unpin,
        Ret: Send + Sync + 'static,
        Err: Send + Sync + 'static,
    {
        let task_id = self.task_id_counter.next();
        if self.closed.load(Ordering::SeqCst) {
            self.tasks.insert(task_id, TaskState::Cancelled);
            self.notify_change();
            let tasks = self.tasks.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                tasks.remove(&task_id);
            });
            return task_id;
        }
        let token = CancellationToken::new();
        let task_token = token.clone();
        let progress = ProgressUpdater {
            tasks: self.tasks.clone(),
            changes: self.changes.clone(),
            task_id,
            _phantom: PhantomData,
        };
        let (death_tx, death_rx) = crossfire::mpmc::Null::new().new_async();
        self.tasks.insert(task_id, TaskState::Pending);
        let tasks_for_result = self.tasks.clone();
        let handles_ref = self.handles.clone();
        let changes = self.changes.clone();
        let worker_handle = tokio::spawn(async move {
            let _guard = TaskGuard {
                task_id,
                tasks: tasks_for_result.clone(),
                handles: handles_ref,
                changes,
                ttl: Duration::from_secs(60),
            };
            let blocking_res = tokio::task::spawn_blocking(move || {
                let _death_tx = death_tx;
                f(progress, task_token)
            })
            .await;
            let terminal_state = match blocking_res {
                Ok(Ok(val)) => TaskState::Completed(Arc::new(val)),
                Ok(Err(err)) => TaskState::Failed(Arc::new(err)),
                Err(join_err) => {
                    let msg = match join_err.try_into_panic() {
                        Ok(panic_err) => panic_payload_to_string(panic_err.as_ref()),
                        Err(_join_err) => "Task was cancelled or aborted".to_string(),
                    };
                    TaskState::Panicked(Arc::new(msg))
                }
            };
            tasks_for_result.alter(&task_id, |_k, v| {
                match v.is_cancelling() || v.is_cancelled() {
                    true => v,
                    false => terminal_state,
                }
            });
        });
        self.handles
            .insert(task_id, (worker_handle, token, Some(death_rx)));
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
        self.tasks.get(&task_id).map(|s| s.value().clone())
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
        if let Some((_, (mut handle, token, death_rx))) = self.handles.remove(&task_id) {
            token.cancel();
            let tasks = self.tasks.clone();
            let changes = self.changes.clone();
            tokio::spawn(async move {
                let graceful_exit = tokio::time::timeout(Duration::from_secs(5), async {
                    match death_rx.as_ref() {
                        Some(rx) => drop(&mut rx.recv().await), // drop() just reduced 2 lines.
                        None => drop((&mut handle).await),
                    }
                })
                .await;
                if graceful_exit.is_err() {
                    handle.abort(); // async task only
                    if let Some(rx) = death_rx.as_ref() {
                        let _ = rx.recv().await; // blocking task only
                    }
                    let _ = handle.await;
                }
                if let Some(mut state) = tasks.get_mut(&task_id) {
                    *state = TaskState::Cancelled;
                }
                let _ = changes.send_modify(|e| *e += 1);
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

    fn notify_change(&self) {
        let _ = self.changes.send_modify(|e| *e += 1);
    }

    /// 等待任务状态满足谓词。
    /// - 只在状态切换时检查，在 Running 更新时不会检查。
    /// - 任务已进入终态时立即返回。
    /// - 任务已被清理时返回 None。
    pub async fn wait_for(
        &self,
        task_id: TaskId,
        mut pred: impl FnMut(&TaskState) -> bool,
    ) -> Option<TaskState> {
        let mut rx = self.changes.subscribe();
        loop {
            if let Some(s) = self.get_status(task_id) {
                if pred(&s) || s.is_terminal() {
                    return Some(s);
                }
            } else {
                return None;
            }
            if rx.changed().await.is_err() {
                return None;
            }
        }
    }

    /// 等待任务进入终态。
    pub async fn wait_terminal(&self, task_id: TaskId) -> Option<TaskState> {
        self.wait_for(task_id, TaskState::is_terminal).await
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

pub struct TaskHandle<Status, Ret, Err> {
    pub id: TaskId,
    tm: TaskManager,
    _phantom: PhantomData<fn() -> (Status, Ret, Err)>,
}

impl<Status, Ret, Err> TaskHandle<Status, Ret, Err>
where
    Status: 'static + Send + Sync,
    Ret: 'static + Send + Sync,
    Err: 'static + Send + Sync,
{
    pub fn new(id: TaskId, tm: TaskManager) -> Self {
        Self {
            id,
            tm,
            _phantom: PhantomData,
        }
    }

    /// 获取任务当前状态。
    pub fn status(&self) -> TypedTaskState<Status, Ret, Err> {
        self.tm
            .get_status(self.id)
            .map(|s| s.to_typed())
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

    /// 等待任务状态满足谓词。
    /// - 只在状态切换时检查，在 Running 更新时不会检查。
    /// - 任务已进入终态时立即返回。
    /// - 任务类型 cast 失败，或被清理时立即返回。
    pub async fn wait_for(
        &self,
        mut pred: impl FnMut(TypedTaskStateRef<Status, Ret, Err>) -> bool,
    ) -> TypedTaskState<Status, Ret, Err> {
        self.tm
            .wait_for(self.id, |s| match s.as_typed::<Status, Ret, Err>() {
                TypedTaskStateRef::Invalid => true, // 类型 cast 失败
                ts => pred(ts),
            })
            .await
            .map(|s| s.to_typed())
            .unwrap_or(TypedTaskState::Invalid)
    }

    /// 等待任务进入终态。
    pub async fn wait_terminal(&self) -> TypedTaskState<Status, Ret, Err> {
        self.tm
            .wait_terminal(self.id)
            .await
            .map(|s| s.to_typed())
            .unwrap_or(TypedTaskState::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 轮询直到 cond 为 true 或超时
    async fn wait_until(cond: impl Fn() -> bool, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if cond() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cond()
    }

    type Status = u32;
    type Ret = i32;
    type Err = String;

    fn mk_tm() -> TaskManager {
        TaskManager::new()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_task_completes() {
        let tm = mk_tm();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(42) });
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "task should complete, got {:?}",
            h.status()
        );
        match h.status() {
            TypedTaskState::Completed(v) => assert_eq!(*v, 42),
            s => panic!("unexpected state: {:?}", s),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_task_fails() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            Err::<Ret, Err>("boom".to_string())
        });
        assert!(
            wait_until(|| h.status().is_failed(), Duration::from_secs(2)).await,
            "task should fail, got {:?}",
            h.status()
        );
        match h.status() {
            TypedTaskState::Failed(e) => assert_eq!(e.as_str(), "boom"),
            s => panic!("unexpected state: {:?}", s),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_task_panics() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            panic!("async panic payload");
            #[allow(unreachable_code)]
            Ok::<Ret, Err>(0)
        });
        assert!(
            wait_until(|| h.status().is_panicked(), Duration::from_secs(2)).await,
            "task should panic, got {:?}",
            h.status()
        );
        match h.status() {
            TypedTaskState::Panicked(msg) => assert!(msg.contains("async panic payload")),
            s => panic!("unexpected state: {:?}", s),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blocking_task_completes_and_panics() {
        let tm = mk_tm();
        let h = tm.spawn_blocking_typed(|_pc: ProgressUpdater<Status>, _ct| Ok::<Ret, Err>(7));
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "blocking task should complete, got {:?}",
            h.status()
        );
        match h.status() {
            TypedTaskState::Completed(v) => assert_eq!(*v, 7),
            s => panic!("unexpected state: {:?}", s),
        }

        let hp = tm.spawn_blocking_typed(|_pc: ProgressUpdater<Status>, _ct| {
            panic!("blocking panic payload");
            #[allow(unreachable_code)]
            Ok::<Ret, Err>(0)
        });
        assert!(
            wait_until(|| hp.status().is_panicked(), Duration::from_secs(2)).await,
            "blocking task should panic, got {:?}",
            hp.status()
        );
        match hp.status() {
            TypedTaskState::Panicked(msg) => assert!(msg.contains("blocking panic payload")),
            s => panic!("unexpected state: {:?}", s),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_pending_to_running_and_update() {
        let tm = mk_tm();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let h = tm.spawn_typed(|pc: ProgressUpdater<Status>, _ct| async move {
            pc.update(1); // Pending -> Running（回归测试 alter 修复）
            tokio::time::sleep(Duration::from_millis(100)).await;
            pc.update(2); // 覆盖 Running 值
            // 等主测试观察到 Running(2) 后再退出
            let _ = release_rx.await;
            let _ = done_tx.send(());
            Ok::<Ret, Err>(0)
        });
        // 第一次 update 后应是 Running(1)
        assert!(
            wait_until(
                || matches!(h.status(), TypedTaskState::Running(v) if *v == 1),
                Duration::from_secs(2)
            )
            .await,
            "should become Running(1), got {:?}",
            h.status()
        );
        // 第二次 update 后应是 Running(2)，且任务还在运行
        assert!(
            wait_until(
                || matches!(h.status(), TypedTaskState::Running(v) if *v == 2),
                Duration::from_secs(2)
            )
            .await,
            "should update Running value to 2, got {:?}",
            h.status()
        );
        // 放行任务完成，终态保留
        let _ = release_tx.send(());
        done_rx.await.unwrap();
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "task should complete, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_does_not_override_cancelling() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|pc: ProgressUpdater<Status>, ct| async move {
            pc.update(1);
            ct.cancelled().await; // 等 cancel_task 把状态置为 Cancelling
            pc.update(2); // 必须被 alter 拒绝（不覆盖 Cancelling）
            Ok::<Ret, Err>(0)
        });
        assert!(
            wait_until(
                || matches!(h.status(), TypedTaskState::Running(v) if *v == 1),
                Duration::from_secs(2)
            )
            .await,
            "should reach Running(1), got {:?}",
            h.status()
        );
        h.cancel();
        // 最终应该是 Cancelled 而非 Running(2)/Completed
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(3)).await,
            "should end Cancelled, got {:?}",
            h.status()
        );
        assert!(
            !matches!(h.status(), TypedTaskState::Running(v) if *v == 2),
            "late progress must not override cancellation"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cooperative_cancel_async() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, ct| async move {
            while !ct.is_cancelled() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<Ret, Err>(1)
        });
        h.cancel();
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(3)).await,
            "should be Cancelled, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "takes ~5s due to cancel_task abort timeout"]
    async fn cancel_aborts_unresponsive_async() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            // 不响应 ct：挂起 1 小时，必须被 abort 干掉
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<Ret, Err>(1)
        });
        h.cancel();
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(8)).await,
            "unresponsive task should be aborted to Cancelled, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cooperative_cancel_blocking() {
        let tm = mk_tm();
        let h = tm.spawn_blocking_typed(|_pc: ProgressUpdater<Status>, ct| {
            while !ct.is_cancelled() {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok::<Ret, Err>(1)
        });
        h.cancel();
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(3)).await,
            "blocking task should be Cancelled, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_after_close_immediately_cancelled() {
        let tm = mk_tm();
        tm.close();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(1) });
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(2)).await,
            "task spawned after close should be Cancelled, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_at_triggers_cancel() {
        let tm = mk_tm();
        let ct = CancellationToken::new();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, task_ct| async move {
            tokio::select! {
                _ = task_ct.cancelled() => {}
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }
            Ok::<Ret, Err>(1)
        });
        h.cancel_at(&ct);
        // 等 watcher 注册后触发
        tokio::time::sleep(Duration::from_millis(100)).await;
        ct.cancel();
        assert!(
            wait_until(|| h.status().is_cancelled(), Duration::from_secs(3)).await,
            "cancel_at should cancel task, got {:?}",
            h.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_status_removes_handle_after_terminal() {
        let tm = mk_tm();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(3) });
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "task should complete"
        );
        // terminal 后 cancel 不应改变状态（handles 已清理，取消视为 no-op）
        h.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(h.status().is_completed(), "terminal state must be sticky");
    }

    // ---- wait_for / wait_terminal 事件驱动等待 ----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_returns_completed_value() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok::<Ret, Err>(42)
        });
        // spawn 后立刻等待（任务还处于 Pending），验证事件通知唤醒而非轮询
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_terminal())
            .await
            .expect("wait_terminal should return promptly");
        match ts {
            TypedTaskState::Completed(v) => assert_eq!(*v, 42),
            s => panic!("expected Completed(42), got {s:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_returns_failed() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            Err::<Ret, Err>("wait fail".to_string())
        });
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_terminal())
            .await
            .expect("wait_terminal should return promptly");
        match ts {
            TypedTaskState::Failed(e) => assert_eq!(e.as_str(), "wait fail"),
            s => panic!("expected Failed, got {s:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_returns_panicked() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            panic!("wait panic payload");
            #[allow(unreachable_code)]
            Ok::<Ret, Err>(0)
        });
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_terminal())
            .await
            .expect("wait_terminal should return promptly");
        match ts {
            TypedTaskState::Panicked(msg) => assert!(msg.contains("wait panic payload")),
            s => panic!("expected Panicked, got {s:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_immediate_when_already_terminal() {
        let tm = mk_tm();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(9) });
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "task should complete first"
        );
        // 已终态: wait 不依赖通知, 立即返回
        let t0 = std::time::Instant::now();
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_terminal())
            .await
            .expect("should return immediately");
        assert!(
            t0.elapsed() < Duration::from_millis(100),
            "should not wait for a notification"
        );
        assert!(matches!(ts, TypedTaskState::Completed(v) if *v == 9));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_on_blocking_task() {
        let tm = mk_tm();
        let h = tm.spawn_blocking_typed(|_pc: ProgressUpdater<Status>, _ct| Ok::<Ret, Err>(7));
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_terminal())
            .await
            .expect("blocking task should finish");
        assert!(matches!(ts, TypedTaskState::Completed(v) if *v == 7));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_then_wait_terminal_returns_cancelled() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, ct| async move {
            ct.cancelled().await; // 协作式: cancel 后立即退出
            Ok::<Ret, Err>(1)
        });
        h.cancel();
        let ts = tokio::time::timeout(Duration::from_secs(3), h.wait_terminal())
            .await
            .expect("cancel should settle promptly");
        assert!(ts.is_cancelled(), "expected Cancelled, got {ts:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancel_unresponsive_blocking_wait_waits_real_exit() {
        let tm = mk_tm();
        // 注意: blocking 任务不 update 时运行期状态一直是 Pending,
        // 不能用状态判断"任务开始执行", 这里用 flag 确认闭包已进入。
        let entered = Arc::new(AtomicBool::new(false));
        let e2 = entered.clone();
        let h = tm.spawn_blocking_typed(move |_pc: ProgressUpdater<Status>, _ct| {
            e2.store(true, Ordering::Release);
            // 不响应 token, 200ms 后自行返回
            std::thread::sleep(Duration::from_millis(200));
            Ok::<Ret, Err>(1)
        });
        // 等闭包真正开始执行
        assert!(
            wait_until(|| entered.load(Ordering::Acquire), Duration::from_secs(2)).await,
            "blocking closure should start"
        );
        let t0 = std::time::Instant::now();
        h.cancel();
        let ts = tokio::time::timeout(Duration::from_secs(3), h.wait_terminal())
            .await
            .expect("wait_terminal should settle after the blocking closure really exits");
        assert!(ts.is_cancelled(), "expected Cancelled, got {ts:?}");
        // Cancelled 必须等 blocking 闭包真正返回(约 200ms), 而不是 abort 包装后就谎报
        assert!(
            t0.elapsed() >= Duration::from_millis(150),
            "Cancelled arrived too early ({:?}), blocking closure had not exited",
            t0.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_observes_cancelling_or_cancelled() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|pc: ProgressUpdater<Status>, ct| async move {
            pc.update(1); // Running
            ct.cancelled().await;
            Ok::<Ret, Err>(1)
        });
        // 等 Running 可见后, 用 wait_for 谓词等待 Cancelling/Cancelled
        assert!(
            wait_until(
                || matches!(h.status(), TypedTaskState::Running(v) if *v == 1),
                Duration::from_secs(2)
            )
            .await,
            "should reach Running(1)"
        );
        h.cancel();
        let ts = tokio::time::timeout(
            Duration::from_secs(3),
            h.wait_for(|s| s.is_cancelling() || s.is_cancelled()),
        )
        .await
        .expect("wait_for should observe cancellation");
        // Cancelling 是瞬态: 谓词可能先命中 Cancelling, 也可能直接被 terminal(Cancelled) 兜底
        assert!(
            ts.is_cancelling() || ts.is_cancelled(),
            "expected Cancelling/Cancelled, got {ts:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_terminal_fallback_when_predicate_never_matches() {
        let tm = mk_tm();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(5) });
        // 谓词永远 false: 任务进入终态后必须兜底返回 terminal, 而不是永久挂起
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_for(|_| false))
            .await
            .expect("terminal fallback should return");
        assert!(
            ts.is_completed(),
            "expected Completed via terminal fallback, got {ts:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_multi_waiter_all_woken() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<Ret, Err>(11)
        });
        let (a, b) = tokio::join!(h.wait_terminal(), h.wait_terminal());
        assert!(matches!(a, TypedTaskState::Completed(v) if *v == 11));
        assert!(matches!(b, TypedTaskState::Completed(v) if *v == 11));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_on_nonexistent_task() {
        let tm = mk_tm();
        let ghost = TaskId::from(999_999);
        // TaskManager 层: 任务不存在 -> None
        let none = tokio::time::timeout(Duration::from_secs(1), tm.wait_terminal(ghost))
            .await
            .expect("must return immediately");
        assert!(none.is_none(), "expected None for nonexistent task");
        // Handle 层: 任务不存在 -> Invalid
        let h = TaskHandle::<Status, Ret, Err>::new(ghost, tm);
        assert!(h.wait_terminal().await.is_invalid());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_terminal_wrong_type_handle_is_invalid() {
        let tm = mk_tm();
        let h =
            tm.spawn_typed(|_pc: ProgressUpdater<Status>, _ct| async move { Ok::<Ret, Err>(3) });
        assert!(
            wait_until(|| h.status().is_completed(), Duration::from_secs(2)).await,
            "task should complete"
        );
        // 用错误泛型构造 handle: downcast 失败 -> Invalid
        let wrong: TaskHandle<String, String, String> = TaskHandle::new(h.id, tm.clone());
        let ts = tokio::time::timeout(Duration::from_secs(1), wrong.wait_terminal())
            .await
            .expect("wrong-typed wait should return immediately");
        assert!(ts.is_invalid(), "expected Invalid, got {ts:?}");
    }

    // ---- TypedTaskStateRef / as_typed / to_typed 转换 ----

    #[test]
    fn typed_state_ref_conversions() {
        // owned (Arc) -> ref (借用)
        let owned = TypedTaskState::Completed(Arc::new(42i32));
        let r: TypedTaskStateRef<u32, i32, String> = (&owned).into();
        assert!(matches!(r, TypedTaskStateRef::Completed(v) if *v == 42));
        // ref -> owned (clone)
        let back: TypedTaskState<u32, i32, String> = r.into();
        assert!(matches!(back, TypedTaskState::Completed(v) if *v == 42));
        // 无 payload 变体往返
        let p: TypedTaskStateRef<u32, i32, String> =
            (&TypedTaskState::<u32, i32, String>::Pending).into();
        assert!(p.is_pending());
        let o: TypedTaskState<u32, i32, String> =
            TypedTaskStateRef::Panicked(&"x".to_string()).into();
        assert!(o.is_panicked());
        let inv_r: TypedTaskStateRef<u32, i32, String> =
            (&TypedTaskState::<u32, i32, String>::Invalid).into();
        assert!(inv_r.is_invalid());
        let inv_o: TypedTaskState<u32, i32, String> = TypedTaskStateRef::Invalid.into();
        assert!(inv_o.is_invalid());
    }

    #[test]
    fn typed_cast_ok_and_mismatch() {
        // as_typed: 类型匹配 -> Running(&v)
        let s = TaskState::Running(Arc::new(7u32));
        assert!(
            matches!(s.as_typed::<u32, i32, String>(), TypedTaskStateRef::Running(v) if *v == 7)
        );
        // as_typed: 类型不匹配 -> Invalid
        assert!(s.as_typed::<String, i32, String>().is_invalid());
        // to_typed: Completed downcast 失败 -> Invalid
        let c = TaskState::Completed(Arc::new(3i32));
        assert!(matches!(
            c.to_typed::<u32, String, String>(),
            TypedTaskState::Invalid
        ));
        // into_result: downcast 失败 / 非终态 -> None
        assert!(c.into_result::<String, String>().is_none());
        assert!(TaskState::Pending.into_result::<i32, String>().is_none());
        // 终态类型匹配 -> Some(Ok/Err)
        assert!(matches!(c.into_result::<i32, String>(), Some(Ok(v)) if *v == 3));
        let f = TaskState::Failed(Arc::new("e".to_string()));
        assert!(
            matches!(f.into_result::<i32, String>(), Some(Err(TaskError::Failed(e))) if e.as_str() == "e")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wait_for_observes_running_on_first_update() {
        let tm = mk_tm();
        let h = tm.spawn_typed(|pc: ProgressUpdater<Status>, _ct| async move {
            tokio::time::sleep(Duration::from_millis(20)).await; // 给 waiter 时间先挂起
            pc.update(1); // Pending -> Running: 切换通知
            pc.update(2); // Running -> Running: 值刷新, 不通知(等 v==2 的谓词不应依赖它)
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<Ret, Err>(0)
        });
        let ts = tokio::time::timeout(Duration::from_secs(2), h.wait_for(|s| s.is_running()))
            .await
            .expect("首次 update 的 Pending->Running 切换应唤醒 waiter");
        match ts {
            // 谓词命中 Running; 值可能已被后续 update 刷新为 2, 但变体必为 Running
            TypedTaskState::Running(v) => assert!(*v == 1 || *v == 2, "unexpected progress {v}"),
            s => panic!("expected Running, got {s:?}"),
        }
    }
}
