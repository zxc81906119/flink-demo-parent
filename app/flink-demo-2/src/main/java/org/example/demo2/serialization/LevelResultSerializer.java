package org.example.demo2.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.example.demo2.model.MonthlyConsumptionFact;

/**
 * 每月信用卡分級結果 JSON 序列化器，供 Kafka Sink 使用。
 */
public class LevelResultSerializer implements SerializationSchema<MonthlyConsumptionFact> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = JsonMapper.builder().build();
    }

    @Override
    public byte[] serialize(MonthlyConsumptionFact fact) {
        if (objectMapper == null) {
            objectMapper = JsonMapper.builder().build();
        }

        try {
            return objectMapper.writeValueAsBytes(fact);
        } catch (Exception e) {
            System.err.println("Failed to serialize monthly consumption fact: " + fact);
            return new byte[0];
        }
    }
}
