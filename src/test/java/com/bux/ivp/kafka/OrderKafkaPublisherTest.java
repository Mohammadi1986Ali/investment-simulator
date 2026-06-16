package com.bux.ivp.kafka;

import com.bux.ivp.config.KafkaConfiguration;
import com.bux.ivp.infrastructure.kafka.event.OrderExecuted;
import com.bux.ivp.infrastructure.kafka.event.OrderFailed;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

@SpringBootTest(classes = KafkaConfiguration.class)
public class OrderKafkaPublisherTest {

    private static final String TOPIC = "order-events";

    @Autowired
    private KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Test
    void produceOrderExecuted() throws Exception {
        UUID orderId = UUID.fromString("42318f90-6460-4028-a322-7316010429ae");
        OrderExecuted orderExecuted = OrderExecuted.newBuilder()
                .setOrderId(orderId)
                .build();

        kafkaTemplate.send(TOPIC, orderId.toString(), orderExecuted).get();
        System.out.println("Sent OrderExecuted with orderId=" + orderId);
    }

    @Test
    void produceOrderFailed() throws Exception {
        UUID orderId = UUID.fromString("d0b8efb6-c0f4-4f2e-af72-c7b15569f7e7");
        OrderFailed orderFailed = OrderFailed.newBuilder()
                .setOrderId(orderId)
                .setReason("some reason")
                .build();

        kafkaTemplate.send(TOPIC, orderId.toString(), orderFailed).get();
        System.out.println("Sent OrderFailed with orderId=" + orderId);
    }
}
