package common;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class StudentScoreGenerators extends RichParallelSourceFunction {
    private volatile Boolean isRunning = true;
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        Random random = new Random();
        while (isRunning) {
            int id = random.nextInt(10);
            int cla = random.nextInt(2);
            int chinese_score = 60 + random.nextInt(90);
            int math_score = 30 + random.nextInt(120);
            int engnish_score = 30 + random.nextInt(120);

            sourceContext.collect(new StudentScore(id, cla, chinese_score, math_score, engnish_score, System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
