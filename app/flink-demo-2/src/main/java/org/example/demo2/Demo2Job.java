package org.example.demo2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.common.model.CreditCardTransaction;
import org.example.common.serialization.TransactionDeserializer;
import org.example.demo2.cdc.CardLimitDebeziumDeserializer;
import org.example.demo2.model.CardLimitUpdate;
import org.example.demo2.model.MonthlyConsumptionFact;
import org.example.demo2.process.MonthlyAggregationFunction;
import org.example.demo2.rule.CardLevelRuleEngine;
import org.example.demo2.serialization.LevelResultSerializer;

import java.time.Duration;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Slf4j
public class Demo2Job {
    public static final DateTimeFormatter YEAR_MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");
    public static final ZoneId ZONE = ZoneId.systemDefault();
    // Kafka 設定：沿用 Main 之來源 topic 與資料結構
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String TRANSACTION_TOPIC = "credit-card-transactions";
    private static final String LEVEL_RESULT_TOPIC = "credit-card-level-results";
    private static final String CONSUMER_GROUP = "monthly-card-level-group";

    // PostgreSQL 設定：對應 scripts/start-flink-cluster.sh 啟動之 postgres 容器
    // 供 Flink CDC 監聽 card_limit 表使用（需 wal_level=logical，見 scripts/start-flink-cluster.sh）
    private static final String POSTGRES_HOSTNAME = "postgres";
    private static final int POSTGRES_PORT = 5432;
    private static final String POSTGRES_DATABASE = "carddb";
    private static final String POSTGRES_SCHEMA = "public";
    private static final String POSTGRES_TABLE = "card_limit";
    private static final String POSTGRES_USERNAME = "carduser";
    private static final String POSTGRES_PASSWORD = "cardpass";
    private static final String POSTGRES_SLOT_NAME = "monthly_card_level_slot";

    private static final int PARALLELISM = 3;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BusinessKey {
        private String cardNumber;
        private String yearMonth;
    }

    /**
     * 依交易的 timestamp 解析所屬月份（yyyyMM），若無 timestamp 則退回目前處理時間。
     */
    public static String resolveYearMonth(long timestamp) {
        // 事件當下時間 , 看語意
        return YearMonth.from(
                        Instant.ofEpochMilli(timestamp)
                                .atZone(ZONE)
                )
                .format(YEAR_MONTH_FORMATTER);
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200L);

        // 1. Kafka Source：與 Main 相同的來源 topic 與反序列化器
        KafkaSource<CreditCardTransaction> kafkaSource = KafkaSource.<CreditCardTransaction>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(TRANSACTION_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        DataStream<CreditCardTransaction> transactionStream = env.fromSource(
                        kafkaSource,
                        // 事件時間 - 3 天 -1 毫秒
                        WatermarkStrategy.<CreditCardTransaction>forBoundedOutOfOrderness(Duration.ofDays(3))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp()),
                        "Kafka Source - Credit Card Transactions"
                )
                .name("Transaction Source");

        // 2. 依卡號分組
        KeyedStream<CreditCardTransaction, BusinessKey> keyedStream = transactionStream
                .keyBy((cardTransaction) -> {
                    BusinessKey businessKey = new BusinessKey(cardTransaction.getCardNumber(), resolveYearMonth(cardTransaction.getTimestamp()));
                    System.out.println(businessKey);
                    return businessKey;
                });

        // 3. card_limit 表 enrichment：透過 Flink CDC 監聽 PostgreSQL WAL 邏輯複製，
        //    轉成 broadcast stream（stream + table 概念，取代定期輪詢或逐筆 JDBC 查詢）
        PostgresSourceBuilder.PostgresIncrementalSource<CardLimitUpdate> cardLimitCdcSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<CardLimitUpdate>builder()
                        .hostname(POSTGRES_HOSTNAME)
                        .port(POSTGRES_PORT)
                        .database(POSTGRES_DATABASE)
                        .schemaList(POSTGRES_SCHEMA)
                        .tableList(POSTGRES_SCHEMA + "." + POSTGRES_TABLE)
                        .username(POSTGRES_USERNAME)
                        .password(POSTGRES_PASSWORD)
                        // todo 不知道是啥
                        .slotName(POSTGRES_SLOT_NAME)
                        // TODO 解碼 plugin , 不知道是啥
                        .decodingPluginName("pgoutput")
                        // TODO 解碼 plugin , 不知道是啥 , 應該很重要
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new CardLimitDebeziumDeserializer())
                        .build();

        DataStreamSource<CardLimitUpdate> cardLimitUpdates = env.fromSource(
                cardLimitCdcSource,
                WatermarkStrategy.<CardLimitUpdate>noWatermarks()
                        .withIdleness(Duration.ofSeconds(5)),
//                WatermarkStrategy.<CardLimitUpdate>forGenerator((ctx) ->
//                        new WatermarkGenerator<CardLimitUpdate>() {
//                            @Override
//                            public void onEvent(CardLimitUpdate event, long eventTimestamp, WatermarkOutput output) {
//                                // 收到數據時不需要做特別處理，或者也可以在這裡直接發送
//                                output.emitWatermark(new Watermark(Long.MAX_VALUE));
//                            }
//
//                            @Override
//                            public void onPeriodicEmit(WatermarkOutput output) {
//                                // 定期發送 Watermark 時，直接發送最大值
//                                output.emitWatermark(new Watermark(Long.MAX_VALUE));
//                            }
//                        }
//                ),
                "PostgreSQL card_limit CDC Source"
        );
        cardLimitUpdates.setParallelism(1);

        BroadcastStream<CardLimitUpdate> cardLimitBroadcastStream =
                cardLimitUpdates.broadcast(MonthlyAggregationFunction.CARD_LIMIT_STATE_DESCRIPTOR);
        // 4. 即時累加當月消費金額，並與 card_limit broadcast state join，月結時交由規則引擎計算分級
        CardLevelRuleEngine ruleEngine = new CardLevelRuleEngine();

        SingleOutputStreamOperator<MonthlyConsumptionFact> levelResultStream = keyedStream
                .connect(cardLimitBroadcastStream)
                .process(new MonthlyAggregationFunction(ruleEngine))
                .name("Monthly Aggregation & Rule Evaluation");

        DataStream<MonthlyAggregationFunction.LateTransactionRecord> lateTransactions =
                levelResultStream.getSideOutput(MonthlyAggregationFunction.LATE_TRANSACTION_TAG);

        lateTransactions.print("Late Transactions").name("Late Transactions Side Output");

        // 5. Kafka Sink：輸出分級結果
        KafkaSink<MonthlyConsumptionFact> kafkaSink = KafkaSink.<MonthlyConsumptionFact>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(LEVEL_RESULT_TOPIC)
                                .setValueSerializationSchema(new LevelResultSerializer())
                                .build()
                )
                .build();

        levelResultStream.print("Level Result Stream").name("Level Result Stream");

        levelResultStream.sinkTo(kafkaSink).name("Kafka Sink - Credit Card Level Results");

        env.execute("Monthly Credit Card Consumption & Level Classification");
    }
}
