package org.example.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.example.model.CreditCardTransaction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 信用卡交易事件 JSON 反序列化器
 * 用于从 Kafka 消息中解析交易事件
 */
public class TransactionDeserializer implements DeserializationSchema<CreditCardTransaction> {
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = JsonMapper.builder().build();
    }

    @Override
    public CreditCardTransaction deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = JsonMapper.builder().build();
        }
        
        if (message == null || message.length == 0) {
            return null;
        }
        
        try {
            return objectMapper.readValue(message, CreditCardTransaction.class);
        } catch (IOException e) {
            // 记录错误日志，但不中断流处理
            System.err.println("Failed to deserialize transaction: " + new String(message, StandardCharsets.UTF_8));
            throw e;
        }
    }

    @Override
    public boolean isEndOfStream(CreditCardTransaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<CreditCardTransaction> getProducedType() {
        return TypeInformation.of(CreditCardTransaction.class);
    }
}

