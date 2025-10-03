package ru.romanov.outbox.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({StreamingProperties.class})
public class StreamingAutoConfiguration {
}
