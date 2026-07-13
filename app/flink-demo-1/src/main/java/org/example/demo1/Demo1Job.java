package org.example.demo1;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.common.model.CreditCardTransaction;
import org.example.common.serialization.TransactionDeserializer;
import org.example.demo1.model.FraudAlert;
import org.example.demo1.serialization.AlertSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Demo1Job {

    // Kafka 配置
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String TRANSACTION_TOPIC = "credit-card-transactions";
    private static final String ALERT_TOPIC = "fraud-alerts";
    private static final String CONSUMER_GROUP = "fraud-detection-group";
    // HDFS Checkpoint 配置
    private static final String CHECKPOINT_PATH = "hdfs://namenode:9000/flink/checkpoints/fraud-detection";

    // 并行度配置（匹配 Kafka partition 数量）
    private static final int PARALLELISM = 3;

    public static void main(String[] args) throws Exception {

        // 1. 创建 Flink 流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置全局并行度
        // todo 由 flink run 指定比較能動態 , 不然會被這邊 override
        env.setParallelism(PARALLELISM);

        // 3. 配置 RocksDB 状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));

        // 4. 配置 Checkpoint（Exactly-Once 语义）
        env.enableCheckpointing(60000); // 每 60 秒执行一次 checkpoint
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000); // 两次 checkpoint 间隔至少 30 秒
        checkpointConfig.setCheckpointTimeout(600000); // checkpoint 超时时间 10 分钟
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 同时只允许一个 checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        ); // Job 取消时保留 checkpoint
        checkpointConfig.setCheckpointStorage(CHECKPOINT_PATH);

        // 5. 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 最多重启 3 次
                Time.of(10, TimeUnit.SECONDS) // 每次重启间隔 10 秒
        ));

        // debug 比較好看 job 圖
//        env.disableOperatorChaining();

        // 6. 创建 Kafka Source
        KafkaSource<CreditCardTransaction> kafkaSource = KafkaSource.<CreditCardTransaction>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(TRANSACTION_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest()) // 从最早的消息开始消费
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        // 7. 从 Kafka 读取交易流（使用 Processing Time，不需要 Watermark）
        DataStream<CreditCardTransaction> transactionStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source - Credit Card Transactions"
                ).filter(txn ->
                        txn != null && txn.getUserId() != null
                )
                .name("Transaction Source");

        // 8. 按用户 ID 分组
        KeyedStream<CreditCardTransaction, String> keyedStream = transactionStream
                .keyBy(CreditCardTransaction::getUserId);

//        keyedStream.print("Keyed Stream");


        // 9. 定义 CEP 模式：严格连续 3 笔交易，Processing Time 窗口 1 分钟
        Pattern<CreditCardTransaction, ?> pattern = Pattern
                .<CreditCardTransaction>begin(
                        "first"
                        , AfterMatchSkipStrategy.skipPastLastEvent()
                )
                .where(new SimpleCondition<>() {
                    @Override
                    public boolean filter(CreditCardTransaction transaction) {
                        return true;
                    }
                })
                .times(3)           // 连续 3 次
                .consecutive()      // 严格连续（中间不能有其他交易）
                .within(Duration.ofMinutes(1));


        // 10. 应用 CEP 模式匹配（使用 Processing Time 语意）
        PatternStream<CreditCardTransaction> patternStream =
                CEP.pattern(keyedStream, pattern)
                        .inProcessingTime();

        // 11. 处理匹配结果，生成告警（带去重机制）
        DataStream<FraudAlert> alertStream = patternStream.process(
                new PatternProcessFunction<CreditCardTransaction, FraudAlert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<CreditCardTransaction>> match,
                            Context ctx,
                            Collector<FraudAlert> out
                    ) {
                        // 获取匹配的 3 笔交易
                        List<CreditCardTransaction> transactions = match.get("first");

                        if (transactions == null || transactions.size() != 3) {
                            return; // 安全检查
                        }

                        // 提取交易信息
                        String userId = transactions.get(0).getUserId();
                        List<String> transactionIds = new ArrayList<>();
                        double totalAmount = 0.0;
                        long firstTimestamp = transactions.get(0).getTimestamp();
                        long lastTimestamp = transactions.get(2).getTimestamp();

                        for (CreditCardTransaction txn : transactions) {
                            transactionIds.add(txn.getTransactionId());
                            totalAmount += txn.getAmount();
                        }

                        // 生成唯一的告警 ID（基于用户 ID + 第一笔交易时间戳）实现去重
                        String alertId = userId + "-" + firstTimestamp;

                        // 构建告警事件
                        FraudAlert alert = new FraudAlert(
                                alertId,
                                userId,
                                "RAPID_CONSECUTIVE_TRANSACTIONS",
                                transactionIds,
                                totalAmount,
                                firstTimestamp,
                                lastTimestamp,
                                System.currentTimeMillis(),
                                String.format("用户 %s 在 1 分钟内发生 3 笔严格连续交易，总金额: %.2f",
                                        userId, totalAmount)
                        );
                        // 输出告警
                        out.collect(alert);
                    }
                }
        ).name("Fraud Alert Generator");

//        alertStream.print("debug");
        // 12. 创建 Kafka Sink
        KafkaSink<FraudAlert> kafkaSink = KafkaSink.<FraudAlert>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ALERT_TOPIC)
                        .setValueSerializationSchema(new AlertSerializer())
                        .build())
                .build();
        // 13. 将告警写入 Kafka
        alertStream.sinkTo(kafkaSink).name("Kafka Sink - Fraud Alerts");

        // 14. 执行 Flink Job
        env.execute("Credit Card Fraud Detection - CEP");
    }
}

