package org.example.demo2.cdc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.Envelope;
import org.example.demo2.model.CardLimitUpdate;

@Slf4j
public class CardLimitDebeziumDeserializer implements DebeziumDeserializationSchema<CardLimitUpdate> {
    private static final long serialVersionUID = 1L;

    private static final String FIELD_CARD_NUMBER = "card_number";
    private static final String FIELD_CREDIT_LIMIT = "credit_limit";

    @Override
    public void deserialize(SourceRecord record, Collector<CardLimitUpdate> out) {
        Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }

        Envelope.Operation op = Envelope.operationFor(record);
        if (op == Envelope.Operation.DELETE) {
            Struct before = value.getStruct(Envelope.FieldName.BEFORE);
            if (before == null) {
                log.warn("收到 delete 事件但缺少 before 資料，略過: record={}", record);
                return;
            }
            String cardNumber = before.getString(FIELD_CARD_NUMBER);
            out.collect(CardLimitUpdate.delete(cardNumber));
            log.info("card_limit CDC 刪除事件: cardNumber={}", cardNumber);
            return;
        }

        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        if (after == null) {
            log.warn("收到非 delete 事件但缺少 after 資料，略過: record={}", record);
            return;
        }

        String cardNumber = after.getString(FIELD_CARD_NUMBER);
        double creditLimit = extractCreditLimit(after);
        out.collect(CardLimitUpdate.upsert(cardNumber, creditLimit));
        log.info("card_limit CDC upsert 事件: cardNumber={}, creditLimit={}, op={}", cardNumber, creditLimit, op);
    }

    /**
     * credit_limit 對應 PostgreSQL NUMERIC 型別，Debezium 預設會轉成
     * java.math.BigDecimal（或依 decimal.handling.mode 設定轉為 String/double），
     * 這裡統一轉為 double 供下游 broadcast state 使用。
     */
    private double extractCreditLimit(Struct after) {
        Field field = after.schema().field(FIELD_CREDIT_LIMIT);
        Object rawValue = after.get(field);
        if (rawValue instanceof Number) {
            return ((Number) rawValue).doubleValue();
        }
        return Double.parseDouble(String.valueOf(rawValue));
    }

    @Override
    public TypeInformation<CardLimitUpdate> getProducedType() {
        return TypeInformation.of(CardLimitUpdate.class);
    }
}
