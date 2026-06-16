package com.bux.ivp.kafka;

import com.bux.ivp.config.KafkaConfiguration;
import com.bux.ivp.infrastructure.kafka.command.CreatePlan;
import com.bux.ivp.infrastructure.kafka.command.DeletePlan;
import com.bux.ivp.infrastructure.kafka.command.ExecutePlan;
import com.bux.ivp.infrastructure.kafka.command.PlanInvestment;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@SpringBootTest(classes = KafkaConfiguration.class)
class CreatePlanKafkaProducerTest {

    private static final String TOPIC = "ivp-commands";

    @Autowired
    private KafkaTemplate<String, SpecificRecord> kafkaTemplate;

    @Test
    void produceCreatePlan() throws Exception {
        CreatePlan createPlan = CreatePlan.newBuilder()
                .setUserId(UUID.randomUUID())
                .setName("My Investment Plan")
                .setInvestments(List.of(
                        PlanInvestment.newBuilder()
                                .setInstrument("AAPL")
                                .setAmount(150.0)
                                .build(),
                        PlanInvestment.newBuilder()
                                .setInstrument("TSLA")
                                .setAmount(75.0)
                                .build()
                ))
                .setExecutionDay(15)
                .build();

        String key = UUID.randomUUID().toString();
        kafkaTemplate.send(TOPIC, key, createPlan).get();
        System.out.println("Sent CreatePlan with key=" + key);
    }

    @Test
    void produceDeletePlan() throws Exception {
        UUID planId = UUID.fromString("2d518b0b-312d-4bb4-ae2d-d7ad5894e96f");
        DeletePlan deletePlan = DeletePlan.newBuilder()
                .setPlanId(planId)
                .build();

        kafkaTemplate.send(TOPIC, planId.toString(), deletePlan).get();
        System.out.println("Sent DeletePlan with planId=" + planId);
    }

    @Test
    void produceExecutePlan() throws Exception {
        UUID planId = UUID.fromString("b72589f2-225b-48b2-afa2-007f324b09c8");
        ExecutePlan executePlan = ExecutePlan.newBuilder()
                .setPlanId(planId)
                .setExecutionDate(LocalDate.now())
                .build();

        kafkaTemplate.send(TOPIC, planId.toString(), executePlan).get();
        System.out.println("Sent ExecutePlan with planId=" + planId + ", date=" + executePlan.getExecutionDate());
    }
}
