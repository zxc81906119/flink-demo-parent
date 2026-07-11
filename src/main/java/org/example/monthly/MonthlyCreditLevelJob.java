package org.example.monthly;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.CreditCardTransaction;
import org.example.monthly.cdc.CardLimitDebeziumDeserializer;
import org.example.monthly.model.CardLimitUpdate;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.process.MonthlyAggregationFunction;
import org.example.monthly.rule.CardLevelRuleEngine;
import org.example.monthly.serialization.LevelResultSerializer;
import org.example.serialization.TransactionDeserializer;

/**
 * 每月信用卡消費統計 + 額度分級 Flink Job。
 *
 * 資料來源沿用 {@link org.example.Main} 相同的 Kafka topic
 * （credit-card-transactions）與 {@link CreditCardTransaction} 資料結構：
 * - 依卡號即時累加當月消費總金額，過程僅記錄 log，不立即輸出。
 * - 月結（處理時間跨過月份邊界）時，從 broadcast state 讀取該卡當月額度上限，
 *   交由 Drools 決策表規則引擎計算分級 status / description。
 * - 分級結果輸出到新 Kafka topic（credit-card-level-results）。
 *
 * 額度上限的 enrichment 採用「stream + table」概念（類似 ksqlDB 的 KTable），
 * 並以 Postgres 邏輯複製（WAL）即時同步，而非定期輪詢：
 * 透過 Flink CDC（{@code flink-connector-postgres-cdc}）監聽 PostgreSQL
 * card_limit 表的異動（INSERT/UPDATE/DELETE），經
 * {@link CardLimitDebeziumDeserializer} 轉為 {@link CardLimitUpdate}，
 * 再以 broadcast stream 廣播給每個 keyed 分區，在本地 broadcast state 中
 * materialize 成一張表，取代逐筆交易觸發一次 JDBC 查詢的作法。
 */
@Slf4j
public class MonthlyCreditLevelJob {

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

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);

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
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source - Credit Card Transactions"
                ).filter(txn -> txn != null && txn.getCardNumber() != null)
                .name("Transaction Source");

        // 2. 依卡號分組
        KeyedStream<CreditCardTransaction, String> keyedStream = transactionStream
                .keyBy(CreditCardTransaction::getCardNumber);

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
                        .slotName(POSTGRES_SLOT_NAME)
                        .decodingPluginName("pgoutput")
                        .startupOptions(StartupOptions.initial())
                        .deserializer(new CardLimitDebeziumDeserializer())
                        .build();

        DataStreamSource<CardLimitUpdate> cardLimitUpdates = env.fromSource(
                cardLimitCdcSource, WatermarkStrategy.noWatermarks(), "PostgreSQL card_limit CDC Source");
        cardLimitUpdates.setParallelism(1);

        BroadcastStream<CardLimitUpdate> cardLimitBroadcastStream =
                cardLimitUpdates.broadcast(MonthlyAggregationFunction.CARD_LIMIT_STATE_DESCRIPTOR);

        // 4. 即時累加當月消費金額，並與 card_limit broadcast state join，月結時交由規則引擎計算分級
        CardLevelRuleEngine ruleEngine = new CardLevelRuleEngine();

        DataStream<MonthlyConsumptionFact> levelResultStream = keyedStream
                .connect(cardLimitBroadcastStream)
                .process(new MonthlyAggregationFunction(ruleEngine))
                .name("Monthly Aggregation & Rule Evaluation");

        // 5. Kafka Sink：輸出分級結果
        KafkaSink<MonthlyConsumptionFact> kafkaSink = KafkaSink.<MonthlyConsumptionFact>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(LEVEL_RESULT_TOPIC)
                        .setValueSerializationSchema(new LevelResultSerializer())
                        .build())
                .build();
        levelResultStream.sinkTo(kafkaSink).name("Kafka Sink - Credit Card Level Results");

        env.execute("Monthly Credit Card Consumption & Level Classification");
    }
}
