package data_stream.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public class AvgScoreFlatmapFunction extends RichFlatMapFunction implements CheckpointedFunction {
    @Override
    public void flatMap(Object o, Collector collector) throws Exception {

    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {

    }
}
