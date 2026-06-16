package com.bux.ivp.simulator;

import com.bux.ivp.infrastructure.kafka.command.CreatePlan;
import com.bux.ivp.infrastructure.kafka.command.DeletePlan;
import com.bux.ivp.infrastructure.kafka.command.ExecutePlan;
import com.bux.ivp.infrastructure.kafka.command.PlanInvestment;
import org.apache.avro.specific.SpecificRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "simulator.enabled", havingValue = "true")
public class CommandSimulator {

    private static final Logger log = LoggerFactory.getLogger(CommandSimulator.class);

    private final KafkaTemplate<String, SpecificRecord> kafkaTemplate;
    private final String topic;

    public CommandSimulator(KafkaTemplate<String, SpecificRecord> kafkaTemplate,
                            @Value("${simulator.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedRateString = "${simulator.interval-ms:5000}")
    public void simulate() {
        int command = ThreadLocalRandom.current().nextInt(3);
        switch (command) {
            case 0 -> sendCreatePlan();
            case 1 -> sendDeletePlan();
            case 2 -> sendExecutePlan();
        }
    }

    private void sendCreatePlan() {
        String planId = UUID.randomUUID().toString();
        CreatePlan createPlan = CreatePlan.newBuilder()
                .setUserId(UUID.randomUUID())
                .setName("Plan-" + planId.substring(0, 8))
                .setInvestments(List.of(
                        PlanInvestment.newBuilder()
                                .setInstrument("AAPL")
                                .setAmount(100.0)
                                .build(),
                        PlanInvestment.newBuilder()
                                .setInstrument("GOOGL")
                                .setAmount(50.0)
                                .build()
                ))
                .setExecutionDay(ThreadLocalRandom.current().nextInt(1, 29))
                .build();

        kafkaTemplate.send(topic, planId, createPlan);
        log.info("Sent CreatePlan: userId={}, name={}", createPlan.getUserId(), createPlan.getName());
    }

    private void sendDeletePlan() {
        UUID planId = UUID.randomUUID();
        DeletePlan deletePlan = DeletePlan.newBuilder()
                .setPlanId(planId)
                .build();

        kafkaTemplate.send(topic, planId.toString(), deletePlan);
        log.info("Sent DeletePlan: planId={}", planId);
    }

    private void sendExecutePlan() {
        UUID planId = UUID.randomUUID();
        ExecutePlan executePlan = ExecutePlan.newBuilder()
                .setPlanId(planId)
                .setExecutionDate(LocalDate.now())
                .build();

        kafkaTemplate.send(topic, planId.toString(), executePlan);
        log.info("Sent ExecutePlan: planId={}, date={}", planId, executePlan.getExecutionDate());
    }
}
