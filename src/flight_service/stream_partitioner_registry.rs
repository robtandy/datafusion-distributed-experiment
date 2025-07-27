use dashmap::{DashMap, Entry};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::common::runtime::{JoinSet, SpawnedTask};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, Partitioning};
use futures::{Stream, StreamExt};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

// TODO: find some way of cleaning up abandoned partitioners

/// Executes an [ExecutionPlan] in the background and re-partitions the data based
/// on the provided [Partitioning] scheme, fanning the data out to N channels, where N
/// is the number of output partitions.
pub struct StreamPartitioner {
    schema: SchemaRef,
    rxs: Mutex<Vec<Option<mpsc::Receiver<Result<RecordBatch, DataFusionError>>>>>,
    _stream_task: SpawnedTask<()>,
}

impl StreamPartitioner {
    /// Builds a new [StreamPartitioner] that executes the provided [ExecutionPlan] and
    /// re-partitions the data using the provided [Partitioning] scheme.
    ///
    /// Building a new [StreamPartitioner] spawns a new background task that:
    /// 1. Executes the input [ExecutionPlan], which provides a data stream.
    /// 2. Re-partitions the data stream based on the [Partitioning] scheme.
    /// 3. Fans-out each partitioned stream to N mpsc queues.
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        ctx: Arc<TaskContext>,
    ) -> Result<Self, DataFusionError> {
        let partitions = partitioning.partition_count();
        let schema = plan.schema();

        let mut rxs = Vec::with_capacity(partitions);
        let mut txs = Vec::with_capacity(partitions);
        let mut partitioner = BatchPartitioner::try_new(partitioning, Default::default())?;
        for _ in 0..partitions {
            // TODO: use some memory aware channels.
            let (tx, rx) = mpsc::channel(1024);
            rxs.push(Some(rx));
            txs.push(tx);
        }

        let stream_task = SpawnedTask::spawn(async move {
            // Executes the plan as normal, returning a single stream of RecordBatches.
            let mut stream = match execute_stream(plan, ctx) {
                Ok(v) => v,
                Err(err) => return fanout_error(&txs, Arc::new(err)).await,
            };

            while let Some(batch) = stream.next().await {
                let batch = match batch {
                    Ok(batch) => batch,
                    // If there was an error in this batch, we want to fanout the error to
                    // all the consumers. Everyone needs to be aware that something bad happened.
                    Err(err) => return fanout_error(&txs, Arc::new(err)).await,
                };

                // Uses the partitioner for re-partitioning the stream, producing N batches
                // for each 1 batch in the input stream.
                let mut join_set = JoinSet::new();
                if let Err(err) = partitioner.partition(batch, |i, batch| {
                    // produce the partitioned RecordBatch in the appropriate partition transmitter.
                    // the rx receiving end consuming partition `i` will see this message soon.
                    let tx = txs[i].clone();
                    join_set.spawn(async move { tx.send(Ok(batch)).await });
                    Ok(())
                }) {
                    return fanout_error(&txs, Arc::new(err)).await;
                }

                // We need to make sure every partitioned batch was correctly sent. If not, we need
                // to communicate to all consumers that something went wrong.
                for res in join_set.join_all().await {
                    if let Err(err) = res {
                        return fanout_error(
                            &txs,
                            Arc::new(DataFusionError::External(Box::new(err))),
                        )
                        .await;
                    }
                }
            }
        });

        Ok(Self {
            _stream_task: stream_task,
            rxs: Mutex::new(rxs),
            schema,
        })
    }

    /// Consumes the provided partition, streaming the content from the receiving end of the channel.
    /// This function can only be called once per partition, upon the second call on the same
    /// partition this function will fail.
    pub fn stream_partition(
        &self,
        partition: usize,
    ) -> Result<impl Stream<Item = Result<RecordBatch, DataFusionError>>, DataFusionError> {
        let mut rxs = self.rxs.lock().unwrap();
        if partition >= rxs.len() {
            return exec_err!(
                "Invalid partition index {partition} with a total amount of partitions of {}",
                rxs.len()
            );
        }

        let Some(rx) = rxs[partition].take() else {
            return exec_err!("Partition {partition} has already been consumed");
        };

        Ok(ReceiverStream::new(rx))
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// Takes a single [DataFusionError] and sends it to all partitions.
async fn fanout_error(
    txs: &[mpsc::Sender<Result<RecordBatch, DataFusionError>>],
    err: Arc<DataFusionError>,
) {
    let mut join_set = JoinSet::new();
    for tx in txs.iter() {
        let tx = tx.clone();
        let err = err.clone();
        join_set.spawn(async move { tx.send(Err(DataFusionError::Shared(err))).await });
    }
    join_set.join_all().await;
}

/// Keeps track of all the [StreamPartitioner] currently running in the program, identifying them
/// by stage id.
#[derive(Default)]
pub struct StreamPartitionerRegistry {
    map: DashMap<(String, usize), Arc<StreamPartitioner>>,
}

impl StreamPartitionerRegistry {
    /// Builds a new [StreamPartitioner] if there was not one for this specific stage id.
    /// If there was already one, return a reference to it.
    pub fn get_or_create_stream_partitioner(
        &self,
        id: String,
        actor_idx: usize,
        plan: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        context: Arc<TaskContext>,
    ) -> Result<Arc<StreamPartitioner>, DataFusionError> {
        match self.map.entry((id, actor_idx)) {
            Entry::Occupied(entry) => Ok(Arc::clone(entry.get())),
            Entry::Vacant(entry) => Ok(Arc::clone(&entry.insert(Arc::new(
                StreamPartitioner::new(plan, partitioning, context)?,
            )))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MockExec;
    use datafusion::arrow::array::UInt32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::col;

    #[tokio::test]
    async fn round_robin_1() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            "test".to_string(),
            0,
            mock_exec(15, 10),
            Partitioning::RoundRobinBatch(PARTITIONS),
            Arc::new(TaskContext::default()),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(
            rows_per_partition,
            vec![20, 20, 20, 20, 20, 10, 10, 10, 10, 10]
        );
        Ok(())
    }

    #[tokio::test]
    async fn round_robin_2() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            "test".to_string(),
            0,
            mock_exec(5, 10),
            Partitioning::RoundRobinBatch(PARTITIONS),
            Arc::new(TaskContext::default()),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(rows_per_partition, vec![10, 10, 10, 10, 10, 0, 0, 0, 0, 0]);
        Ok(())
    }

    #[tokio::test]
    async fn hash_1() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            "test".to_string(),
            0,
            mock_exec(15, 10),
            Partitioning::Hash(vec![col("c0", &test_schema())?], PARTITIONS),
            Arc::new(TaskContext::default()),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(
            rows_per_partition,
            vec![30, 15, 0, 45, 0, 15, 0, 15, 15, 15]
        );
        Ok(())
    }

    #[tokio::test]
    async fn hash_2() -> Result<(), Box<dyn std::error::Error>> {
        const PARTITIONS: usize = 10;

        let registry = StreamPartitionerRegistry::default();
        let partitioner = registry.get_or_create_stream_partitioner(
            "test".to_string(),
            0,
            mock_exec(5, 10),
            Partitioning::Hash(vec![col("c0", &test_schema())?], PARTITIONS),
            Arc::new(TaskContext::default()),
        )?;

        let rows_per_partition = gather_rows_per_partition(&partitioner).await;

        assert_eq!(
            rows_per_partition,
            vec![10, 5, 0, 15, 0, 5, 0, 5, 5, 5]
        );
        Ok(())
    }

    async fn gather_rows_per_partition(partitioner: &StreamPartitioner) -> Vec<usize> {
        let mut data = vec![];
        let n_partitions = partitioner.rxs.lock().unwrap().len();
        for i in 0..n_partitions {
            let mut stream = partitioner.stream_partition(i).unwrap();
            data.push(0);
            while let Some(msg) = stream.next().await {
                data[i] += msg.unwrap().num_rows();
            }
        }
        data
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c0", DataType::UInt32, false)]))
    }

    fn mock_exec(n_batches: usize, n_rows: usize) -> Arc<dyn ExecutionPlan> {
        Arc::new(MockExec::new(
            create_vec_batches(n_batches, n_rows),
            test_schema(),
        ))
    }

    /// Create vector batches
    fn create_vec_batches(
        n_batches: usize,
        n_rows: usize,
    ) -> Vec<Result<RecordBatch, DataFusionError>> {
        let batch = create_batch(n_rows);
        (0..n_batches).map(|_| Ok(batch.clone())).collect()
    }

    /// Create batch
    fn create_batch(n_rows: usize) -> RecordBatch {
        let schema = test_schema();
        let mut data = vec![];
        for i in 0..n_rows {
            data.push(i as u32)
        }
        RecordBatch::try_new(schema, vec![Arc::new(UInt32Array::from(data))]).unwrap()
    }
}
