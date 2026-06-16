package com.bux.ivp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "simulator.kafka")
public record KafkaProperties(
        String bootstrapServers,
        String schemaRegistryUrl
) {
}
