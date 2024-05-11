package data_stream;

import common.StudentScore;
import common.StudentScoreGenerators;
import common.SubjectAvgScore;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.stream.Collectors;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class TopAverageScore {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<StudentScore> scores =  env.addSource(new StudentScoreGenerators())
                .returns(StudentScore.class)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<StudentScore>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((studentScore, l) -> studentScore.ts)
                );
//        scores.writeAsText().writeAsText("file:///Users/chen.zhao5/Desktop/flink/student_score.txt");
//        scores.keyBy("id")
//                .flatMap(new RichFlatMapFunction<StudentScore, SubjectAvgScore>() {
//                    private MapState<String, HashMap<Integer, Pair<Integer, Integer>>> mapState = null;
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
//                        final MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("mapState", String.class, HashMap.class);
//                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
//                    }
//
//                    @Override
//                    public void flatMap(StudentScore o, Collector collector) throws Exception {
//                        String[] subjects = new String[]{"Chinese", "Math", "English"};
//
//                        for (String subject: subjects) {
//                            if (!mapState.contains(subject)) {
//                                mapState.put(subject, new HashMap());
//                            }
//                            Pair<Integer, Integer> pair = null;
//                            if (!mapState.get(subject).containsKey(o.id)) {
//                                if (subject.equals("Chinese")) {
//                                    pair = new MutablePair(o.chinese_score, 1);
//                                } else if (subject.equals("Math")) {
//                                    pair = new MutablePair(o.math_score, 1);
//                                } else {
//                                    pair = new MutablePair(o.english_score, 1);
//                                }
//                                mapState.get(subject).put(o.id, pair);
//                            } else {
//                                if (subject.equals("Chinese")) {
//                                    pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.chinese_score,  mapState.get(subject).get(o.id).getRight() + 1);
//                                } else if (subject.equals("Math")) {
//                                    pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.math_score,  mapState.get(subject).get(o.id).getRight() + 1);
//                                } else {
//                                    pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.english_score,  mapState.get(subject).get(o.id).getRight() + 1);
//                                }
//                                mapState.get(subject).put(o.id, pair);
//                            }
//                            collector.collect(new SubjectAvgScore(subject, o.id,Double.valueOf(pair.getLeft()) / pair.getRight()));
//                        }
//                    }
//                }).keyBy("subject")
//                .countWindow(30)
//                .maxBy("avg_score")
//                .print();

        scores.keyBy("id").process(new ProcessFunction<StudentScore, SubjectAvgScore>() {
            private MapState<String, HashMap<Integer, Pair<Integer, Integer>>> mapState = null;
            String[] subjects = new String[]{"Chinese", "Math", "English"};
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                final MapStateDescriptor mapStateDescriptor = new MapStateDescriptor("mapState", String.class, HashMap.class);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);
            }

            @Override
            public void processElement(StudentScore o, ProcessFunction<StudentScore, SubjectAvgScore>.Context ctx, Collector<SubjectAvgScore> out) throws Exception {
                if (mapState.isEmpty()) {
                    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000);
                }
                for (String subject: subjects) {
                    if (!mapState.contains(subject)) {
                        mapState.put(subject, new HashMap());
                    }
                    Pair<Integer, Integer> pair = null;
                    if (!mapState.get(subject).containsKey(o.id)) {
                        if (subject.equals("Chinese")) {
                            pair = new MutablePair(o.chinese_score, 1);
                        } else if (subject.equals("Math")) {
                            pair = new MutablePair(o.math_score, 1);
                        } else {
                            pair = new MutablePair(o.english_score, 1);
                        }
                        mapState.get(subject).put(o.id, pair);
                    } else {
                        if (subject.equals("Chinese")) {
                            pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.chinese_score,  mapState.get(subject).get(o.id).getRight() + 1);
                        } else if (subject.equals("Math")) {
                            pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.math_score,  mapState.get(subject).get(o.id).getRight() + 1);
                        } else {
                            pair = new MutablePair(mapState.get(subject).get(o.id).getLeft() + o.english_score,  mapState.get(subject).get(o.id).getRight() + 1);
                        }
                        mapState.get(subject).put(o.id, pair);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<StudentScore, SubjectAvgScore>.OnTimerContext ctx, Collector<SubjectAvgScore> out) throws Exception {
                for (String subject : subjects) {
                    Tuple2<Integer, Double> r = mapState.get(subject).entrySet().stream().map(x -> new Tuple2<Integer, Double>(x.getKey(), Double.valueOf(x.getValue().getLeft()) / x.getValue().getRight())).max((x, y) -> x.f1.compareTo(y.f1)).get();
                    out.collect(new SubjectAvgScore(subject, r.f0, r.f1));
                }
                mapState.clear();
            }
        }).print();
        env.execute("aaa");
    }



}
