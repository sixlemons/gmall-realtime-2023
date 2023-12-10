package com.atguigu.gmall.realtime.app.dim;



import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scopt.Check;

import java.util.concurrent.TimeUnit;

/**
 * @Title: DimSinkAPP
 * @Author ning
 * @Package com.atguigu.gmall.realtime.app.dim
 * @Date 2023/12/10 21:48
 * @description: 读取kafka的数据，并进行数据格式转换+数据清洗
 */
public class DimSinkAPP {
    public static void main(String[] args) {
        //1.准备流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //检查点和状态后端的设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.failureRateRestart(10, Time.of(1L, TimeUnit.DAYS),Time.of(3L,TimeUnit.MINUTES)));
    }
}
