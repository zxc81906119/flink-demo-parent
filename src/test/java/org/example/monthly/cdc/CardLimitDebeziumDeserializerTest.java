package org.example.monthly.cdc;

import io.debezium.data.Envelope;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.example.monthly.model.CardLimitUpdate;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 驗證 CardLimitDebeziumDeserializer 能正確將 Debezium change event
 * （insert / update / delete / 初始 snapshot read）轉換為 CardLimitUpdate。
 *
 * 由於此環境無法啟動真正具備邏輯複製的 PostgreSQL 容器做端對端 CDC 整合測試，
 * 這裡改為直接建構符合 Debezium Envelope 格式的 SourceRecord 進行單元測試，
 * 驗證轉換邏輯本身的正確性。
 */
class CardLimitDebeziumDeserializerTest {

    private static final String ROW_SCHEMA_NAME = "card_limit.Value";

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .name(ROW_SCHEMA_NAME)
            .field("card_number", Schema.STRING_SCHEMA)
            .field("credit_limit", Schema.FLOAT64_SCHEMA)
            .build();

    private static final Envelope ENVELOPE = Envelope.defineSchema()
            .withName("card_limit.Envelope")
            .withRecord(ROW_SCHEMA)
            .withSource(SchemaBuilder.struct().name("source").build())
            .build();

    private final CardLimitDebeziumDeserializer deserializer = new CardLimitDebeziumDeserializer();

    @Test
    void shouldConvertCreateEventToUpsert() throws Exception {
        Struct after = new Struct(ROW_SCHEMA).put("card_number", "CARD_A").put("credit_limit", 300000.0);
        SourceRecord record = toSourceRecord(ENVELOPE.create(after, sourceStruct(), Instant.now()));

        List<CardLimitUpdate> collected = collect(record);

        assertEquals(1, collected.size());
        CardLimitUpdate update = collected.get(0);
        assertEquals("CARD_A", update.getCardNumber());
        assertEquals(300000.0, update.getCreditLimit());
        assertFalse(update.isDeleted());
    }

    @Test
    void shouldConvertReadSnapshotEventToUpsert() throws Exception {
        Struct after = new Struct(ROW_SCHEMA).put("card_number", "CARD_SNAPSHOT").put("credit_limit", 500000.0);
        SourceRecord record = toSourceRecord(ENVELOPE.read(after, sourceStruct(), Instant.now()));

        List<CardLimitUpdate> collected = collect(record);

        assertEquals(1, collected.size());
        CardLimitUpdate update = collected.get(0);
        assertEquals("CARD_SNAPSHOT", update.getCardNumber());
        assertEquals(500000.0, update.getCreditLimit());
        assertFalse(update.isDeleted());
    }

    @Test
    void shouldConvertUpdateEventToUpsertWithNewValue() throws Exception {
        Struct before = new Struct(ROW_SCHEMA).put("card_number", "CARD_B").put("credit_limit", 100000.0);
        Struct after = new Struct(ROW_SCHEMA).put("card_number", "CARD_B").put("credit_limit", 200000.0);
        SourceRecord record = toSourceRecord(ENVELOPE.update(before, after, sourceStruct(), Instant.now()));

        List<CardLimitUpdate> collected = collect(record);

        assertEquals(1, collected.size());
        CardLimitUpdate update = collected.get(0);
        assertEquals("CARD_B", update.getCardNumber());
        assertEquals(200000.0, update.getCreditLimit());
        assertFalse(update.isDeleted());
    }

    @Test
    void shouldConvertDeleteEventToDeleteMarker() throws Exception {
        Struct before = new Struct(ROW_SCHEMA).put("card_number", "CARD_C").put("credit_limit", 400000.0);
        SourceRecord record = toSourceRecord(ENVELOPE.delete(before, sourceStruct(), Instant.now()));

        List<CardLimitUpdate> collected = collect(record);

        assertEquals(1, collected.size());
        CardLimitUpdate update = collected.get(0);
        assertEquals("CARD_C", update.getCardNumber());
        assertTrue(update.isDeleted());
    }

    private List<CardLimitUpdate> collect(SourceRecord record) throws Exception {
        List<CardLimitUpdate> collected = new ArrayList<>();
        Collector<CardLimitUpdate> collector = new Collector<CardLimitUpdate>() {
            @Override
            public void collect(CardLimitUpdate record) {
                collected.add(record);
            }

            @Override
            public void close() {
                // no-op
            }
        };
        deserializer.deserialize(record, collector);
        return collected;
    }

    private Struct sourceStruct() {
        return new Struct(SchemaBuilder.struct().name("source").build());
    }

    private SourceRecord toSourceRecord(Struct envelopeValue) {
        return new SourceRecord(
                null, null, "card_limit", null, null, envelopeValue.schema(), envelopeValue);
    }
}
