mod common;

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::common::localhost::start_localhost_context;
    use crate::common::parquet::register_parquet_tables;
    use crate::common::plan::distribute_aggregate;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion_distributed_experiment::create_task;
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn distributed_aggregation() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context([50050, 50051, 50052]).await;
        register_parquet_tables(&ctx).await?;

        let df = ctx
            .sql(r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#)
            .await?;
        let physical = df.create_physical_plan().await?;

        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        let task = create_task(physical, 3)?;

        let task_str = format!("{:?}", task.displayable());

        assert_snapshot!(physical_str,
            @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], CPUs), input_partitions=CPUs
                      RepartitionExec: partitioning=RoundRobinBatch(CPUs), input_partitions=1
                        AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                          DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        assert_snapshot!(task_str,
            @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  ArrowFlightReadExec: input_actors=8
                    CoalesceBatchesExec: target_batch_size=8192
                      RepartitionExec: partitioning=Hash([RainToday@0], CPUs), input_partitions=CPUs
                        RepartitionExec: partitioning=RoundRobinBatch(CPUs), input_partitions=1
                          AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                            ArrowFlightReadExec: input_actors=1 hash=[RainToday@0]
                              DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        Ok(())
    }
}
