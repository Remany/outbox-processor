package ru.romanov.outbox.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import ru.romanov.outbox.configuration.kafka.DynamicKafkaProducerConfiguration;
import ru.romanov.outbox.configuration.kafka.KafkaTemplateFactory;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.domain.repository.impl.OutboxMessageRepositoryImpl;
import ru.romanov.outbox.facade.StreamingFacade;
import ru.romanov.outbox.facade.impl.OutboxStreamingFacade;
import ru.romanov.outbox.processing.OutboxProcessor;
import ru.romanov.outbox.processing.impl.OutboxProcessorImpl;
import ru.romanov.outbox.scheduler.OutboxCleanJob;
import ru.romanov.outbox.scheduler.impl.OutboxCleanJobImpl;
import ru.romanov.outbox.service.OutboxMessageService;
import ru.romanov.outbox.service.OutboxRecoveryService;
import ru.romanov.outbox.service.impl.OutboxMessageServiceImpl;
import ru.romanov.outbox.service.impl.StartupOutboxRecoveryService;
import ru.romanov.outbox.storage.OutboxMessageQueue;
import ru.romanov.outbox.storage.impl.OutboxMessageQueueImpl;

import javax.sql.DataSource;
import java.util.concurrent.Executor;

@Configuration
@ConditionalOnProperty(
        value = {"streaming.enabled"},
        havingValue = "true",
        matchIfMissing = true)
@Import({DynamicKafkaProducerConfiguration.class})
@EnableConfigurationProperties(StreamingProperties.class)
public class StreamingAutoConfiguration {

    @Bean
    public OutboxMessageQueue miniOutboxQueue() {
        return new OutboxMessageQueueImpl();
    }

    @Bean
    public Executor messagesExecutor(StreamingProperties properties) {
        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        final int concurrency = properties.getOutbox().getConcurrency();
        executor.setCorePoolSize(concurrency);
        executor.setMaxPoolSize(concurrency);
        executor.setQueueCapacity(100);
        executor.setKeepAliveSeconds(60);
        executor.setAllowCoreThreadTimeOut(true);
        executor.setThreadNamePrefix("outbox-messages-executor-");
        executor.initialize();
        return executor;
    }

    @Bean
    public OutboxProcessor miniOutboxProcessor(OutboxMessageQueue outboxQueue, KafkaTemplateFactory kafkaTemplateFactory, @Qualifier("messagesExecutor") Executor messagesExecutor, OutboxMessageService service, StreamingProperties properties, ObjectMapper objectMapper) {
        return new OutboxProcessorImpl(outboxQueue, kafkaTemplateFactory, messagesExecutor, service, properties, objectMapper);
    }

    @Bean
    public OutboxRecoveryService outboxRecoveryService(OutboxMessageRepository repository, OutboxMessageQueue queue, StreamingProperties properties) {
        return new StartupOutboxRecoveryService(repository, queue, properties);
    }

    @Bean
    public StreamingFacade embeddedStreamingDataProcessor(OutboxMessageService outboxService) {
        return new OutboxStreamingFacade(outboxService);
    }

    @Bean
    public OutboxMessageService outboxMessageService(StreamingProperties streamingProperties, OutboxMessageRepository repository, OutboxMessageQueue outboxQueue, ObjectMapper objectMapper) {
        return new OutboxMessageServiceImpl(streamingProperties, repository, outboxQueue, objectMapper);
    }


    @Bean
    public OutboxMessageRepository outboxMessageRepository(NamedParameterJdbcTemplate jdbcTemplate, StreamingProperties properties) {
        return new OutboxMessageRepositoryImpl(jdbcTemplate, properties);
    }

    @Bean
    @Primary
    @ConditionalOnBean(DataSource.class)
    @ConditionalOnMissingBean(NamedParameterJdbcTemplate.class)
    public NamedParameterJdbcTemplate namedJdbcTemplate(final DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public OutboxCleanJob miniOutboxCleanerService(OutboxMessageRepository repository, StreamingProperties properties) {
        return new OutboxCleanJobImpl(repository, properties);
    }
}
