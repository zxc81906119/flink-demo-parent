package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.example.model.FraudAlert;

/**
 * 欺诈告警事件 JSON 序列化器
 * 用于将告警事件发送到 Kafka
 */
public class AlertSerializer implements SerializationSchema<FraudAlert> {
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = JsonMapper.builder().build();
    }

    @Override
    public byte[] serialize(FraudAlert alert) {
        if (objectMapper == null) {
            objectMapper = JsonMapper.builder().build();
        }
        
        try {
            return objectMapper.writeValueAsBytes(alert);
        } catch (Exception e) {
            System.err.println("Failed to serialize alert: " + alert);
            e.printStackTrace();
            return new byte[0];
        }
    }
}

