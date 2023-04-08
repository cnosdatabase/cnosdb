pub mod trigger;

use core::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::from_slice::FromSlice;
use datafusion::physical_plan::displayable;
use futures::TryStreamExt;
use models::runtime::executor::{DedicatedExecutor, Job};
use parking_lot::Mutex;
use spi::query::datasource::stream::StreamProviderRef;
use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecution, QueryStateMachineRef, QueryType};
use spi::query::logical_planner::QueryPlan;
use spi::query::physical_planner::PhysicalPlanner;
use spi::query::scheduler::SchedulerRef;
use spi::query::stream::watermark_tracker::WatermarkTrackerRef;
use spi::Result;
use trace::{error, warn};

use self::trigger::executor::TriggerExecutorRef;
use crate::extension::physical::optimizer_rule::add_state_store::AddStateStore;
use crate::extension::physical::transform_rule::stream_scan::StreamScanPlanner;
use crate::extension::physical::transform_rule::watermark::WatermarkPlanner;
use crate::sql::logical::optimizer::{DefaultLogicalOptimizer, LogicalOptimizer};
use crate::sql::physical::optimizer::PhysicalOptimizer;
use crate::sql::physical::planner::DefaultPhysicalPlanner;
use crate::stream::offset_tracker::{OffsetTracker, OffsetTrackerRef};
use crate::stream::state_store::memory::MemoryStateStoreFactory;
use crate::stream::state_store::StateStoreFactory;

pub struct MicroBatchStreamExecution {
    query_state_machine: QueryStateMachineRef,
    plan: Arc<QueryPlan>,
    stream_providers: Vec<StreamProviderRef>,
    scheduler: SchedulerRef,
    trigger_executor: TriggerExecutorRef,
    state_store_factory: Arc<MemoryStateStoreFactory>,
    watermark_tracker: WatermarkTrackerRef,
    runtime: Arc<DedicatedExecutor>,
    abort_handle: Mutex<Option<Job<()>>>,
}

impl MicroBatchStreamExecution {
    pub fn new(
        query_state_machine: QueryStateMachineRef,
        plan: Arc<QueryPlan>,
        stream_providers: Vec<StreamProviderRef>,
        scheduler: SchedulerRef,
        trigger_executor: TriggerExecutorRef,
        runtime: Arc<DedicatedExecutor>,
    ) -> Self {
        Self {
            query_state_machine,
            plan,
            stream_providers,
            scheduler,
            trigger_executor,
            watermark_tracker: WatermarkTrackerRef::default(),
            state_store_factory: Arc::new(MemoryStateStoreFactory::default()),
            runtime,
            abort_handle: Mutex::new(None),
        }
    }
}

impl MicroBatchStreamExecution {
    fn run_stream(&self) -> Result<Job<()>> {
        let query_state_machine = self.query_state_machine.clone();
        let plan = self.plan.clone();
        let scheduler = self.scheduler.clone();
        let stream_providers = self.stream_providers.clone();
        let watermark_tracker = self.watermark_tracker.clone();
        let state_store_factory = self.state_store_factory.clone();
        let runtime = self.runtime.clone();
        let offset_tracker = Arc::new(OffsetTracker::new());

        let result = self.trigger_executor.schedule(move |current_batch_id| {
            let exec = IncrementalExecution {
                query_state_machine: query_state_machine.clone(),
                plan: plan.clone(),
                scheduler: scheduler.clone(),
                current_batch_id,
                stream_providers: stream_providers.clone(),
                watermark_tracker: watermark_tracker.clone(),
                state_store_factory: state_store_factory.clone(),
                offset_tracker: offset_tracker.clone(),
            };

            runtime
                .spawn(async move {
                    exec.execute().await.map_err(|err| {
                        error!("Execute stream query error: {err}");
                        err
                    })
                })
                .detach();
        });

        Ok(result)
    }
}

async fn update_available_offsets(
    offset_tracker: OffsetTrackerRef,
    stream_providers: &[StreamProviderRef],
) -> DFResult<()> {
    for s in stream_providers {
        let offset = s.latest_available_offset().await?;
        if let Some(offset) = offset {
            offset_tracker.update_available_offset(s.id(), offset);
        }
    }

    Ok(())
}

#[async_trait]
impl QueryExecution for MicroBatchStreamExecution {
    fn query_type(&self) -> QueryType {
        QueryType::Stream
    }

    async fn start(&self) -> Result<Output> {
        let join_handle = self.run_stream()?;

        *self.abort_handle.lock() = Some(join_handle);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "query_id",
            DataType::Utf8,
            false,
        )]));
        let id = self.query_state_machine.query_id.to_string();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from_slice([id]))],
        )?;
        Ok(Output::StreamData(schema, vec![batch]))
    }

    fn cancel(&self) -> Result<()> {
        trace::debug!(
            "Cancel sql query execution: query_id: {:?}, sql: {}, state: {:?}",
            &self.query_state_machine.query_id,
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );

        // change state
        self.query_state_machine.cancel();
        // stop future task
        *self.abort_handle.lock() = None;

        trace::info!(
            "Canceled sql query execution: query_id: {:?}, sql: {}, state: {:?}",
            &self.query_state_machine.query_id,
            self.query_state_machine.query.content(),
            self.query_state_machine.state()
        );
        Ok(())
    }

    fn info(&self) -> QueryInfo {
        let qsm = &self.query_state_machine;
        QueryInfo::new(
            qsm.query_id,
            qsm.query.content().to_string(),
            *qsm.session.tenant_id(),
            qsm.session.tenant().to_string(),
            qsm.query.context().user_info().desc().clone(),
        )
    }

    fn status(&self) -> QueryStatus {
        QueryStatus::new(
            self.query_state_machine.state().clone(),
            self.query_state_machine.duration(),
        )
    }
}

struct IncrementalExecution<T> {
    query_state_machine: QueryStateMachineRef,
    plan: Arc<QueryPlan>,
    scheduler: SchedulerRef,
    current_batch_id: i64,
    stream_providers: Vec<StreamProviderRef>,
    watermark_tracker: WatermarkTrackerRef,
    state_store_factory: Arc<T>,
    offset_tracker: OffsetTrackerRef,
}

impl<T> IncrementalExecution<T>
where
    T: StateStoreFactory + Send + Sync + Debug + 'static,
    T::SS: Send + Sync + Debug,
{
    async fn execute(&self) -> Result<()> {
        // 1. Traverse the data source list of the execution plan, check whether there is new data, and update offset_tracker
        update_available_offsets(self.offset_tracker.clone(), &self.stream_providers).await?;
        trace::trace!("Traverse the data source list of the execution plan, check whether there is new data, and update offset_tracker");

        // 2. Exit this execution if there is no new data
        if !self.offset_tracker.has_available_offsets() {
            trace::trace!("Exit this execution if there is no new data");
            return Ok(());
        }

        self.execute_once().await
    }

    async fn execute_once(&self) -> Result<()> {
        let session = &self.query_state_machine.session;
        let current_watermark_ns = self.watermark_tracker.current_watermark_ns();
        let available_offsets = self.offset_tracker.available_offsets();
        let id = self.query_state_machine.query_id;
        let logical_plan = &self.plan.df_plan;
        trace::trace!(
            "query_id({}), current_batch_id({}), current_watermark_ns({}), available_offsets: {:?}",
            id,
            self.current_batch_id,
            current_watermark_ns,
            available_offsets,
        );

        let logical_optimizer = DefaultLogicalOptimizer::default();
        let opt_plan = logical_optimizer.optimize(logical_plan, session)?;
        trace::debug!(
            "Final stream optimized logical plan:\n{}",
            opt_plan.display_indent_schema()
        );

        let mut phy_planner = DefaultPhysicalPlanner::default();
        // 4. Traverse and replace the TableScan nodes in the execution plan according to the mapping from the data source to the offset range
        trace::trace!(
            "Traverse and replace the TableScan nodes in the execution plan according to the mapping from the data source to the offset range"
        );
        phy_planner
            .inject_physical_transform_rule(Arc::new(StreamScanPlanner::new(available_offsets)));
        phy_planner.inject_physical_transform_rule(Arc::new(WatermarkPlanner::new(
            self.watermark_tracker.clone(),
        )));

        phy_planner.inject_optimizer_rule(Arc::new(AddStateStore::new(
            current_watermark_ns,
            self.state_store_factory.clone(),
        )));

        let exec_plan = phy_planner.create_physical_plan(&opt_plan, session).await?;
        trace::debug!(
            "Final stream physical plan:\nOutput partition count: {}\n{}\n",
            exec_plan.output_partitioning().partition_count(),
            displayable(exec_plan.as_ref()).indent()
        );

        let mut stream = self
            .scheduler
            .schedule(exec_plan, session.inner().task_ctx())
            .await?
            .stream();

        loop {
            match stream.try_next().await {
                Ok(Some(batch)) => {
                    trace::trace!("Receive an item, num rows: {}", batch.num_rows());
                }
                Ok(None) => {
                    break;
                }
                Err(err) => {
                    // TODO Record failed status
                    warn!("Failed run stream query {:?}, error: {}", id, err);
                    break;
                }
            }
        }

        // 6. Record the commit log after the execution is complete
        trace::trace!("Record the commit log after the execution is complete");
        let after_process_watermark_ns = self.watermark_tracker.current_watermark_ns();
        if after_process_watermark_ns > current_watermark_ns {
            // TODO 此处是为了兼容tskv未实现的功能，后续需要修改
            // 处理完一个批次后watermark更新了，则对offset_tracker进行提交
            // 如果没更新则说明没有处理数据
            self.offset_tracker.commit(after_process_watermark_ns);
        }

        Ok(())
    }
}
