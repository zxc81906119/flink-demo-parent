package org.example.monthly;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.model.CreditCardTransaction;
import org.example.monthly.model.MonthlyConsumptionFact;
import org.example.monthly.process.MonthlyAggregationFunction;
import org.example.monthly.repository.CardLimitRepository;
import org.example.monthly.rule.CardLevelRuleEngine;
import org.example.monthly.serialization.LevelResultSerializer;
import org.example.serialization.TransactionDeserializer;

/**
 * 每月信用卡消費統計 + 額度分級 Flink Job。
 *
 * 資料來源沿用 {@link org.example.Main} 相同的 Kafka topic
 * （credit-card-transactions）與 {@link CreditCardTransaction} 資料結構：
 * - 依卡號即時累加當月消費總金額，過程僅記錄 log，不立即輸出。
 * - 月結（處理時間跨過月份邊界）時，查詢 PostgreSQL 取得該卡當月額度上限，
 *   交由 Drools 決策表規則引擎計算分級 status / description。
 * - 分級結果輸出到新 Kafka topic（credit-card-level-results）。
 */
@Slf4j
public class MonthlyCreditLevelJob {

    // Kafka 設定：沿用 Main 之來源 topic 與資料結構
    private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:9092";
    private static final String TRANSACTION_TOPIC = "credit-card-transactions";
    private static final String LEVEL_RESULT_TOPIC = "credit-card-level-results";
    private static final String CONSUMER_GROUP = "monthly-card-level-group";

    // PostgreSQL 設定：對應 scripts/start-flink-cluster.sh 啟動之 postgres 容器
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/carddb";
    private static final String JDBC_USERNAME = "carduser";
    private static final String JDBC_PASSWORD = "cardpass";

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

        // 3. 即時累加當月消費金額，月結時交由規則引擎計算分級
        CardLimitRepository cardLimitRepository = new CardLimitRepository(JDBC_URL, JDBC_USERNAME, JDBC_PASSWORD);
        CardLevelRuleEngine ruleEngine = new CardLevelRuleEngine();

        DataStream<MonthlyConsumptionFact> levelResultStream = keyedStream
                .process(new MonthlyAggregationFunction(cardLimitRepository, ruleEngine))
                .name("Monthly Aggregation & Rule Evaluation");

        // 4. Kafka Sink：輸出分級結果
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
