package ru.romanov.outbox.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import ru.romanov.outbox.metric.OutboxMetrics;
import ru.romanov.outbox.processing.AbstractOutboxProcessor;
import ru.romanov.outbox.processing.impl.EventOrientedOutboxProcessorImpl;
import ru.romanov.outbox.processing.impl.ScheduledOutboxProcessor;
import ru.romanov.outbox.scheduler.OutboxCleanJob;
import ru.romanov.outbox.scheduler.impl.OutboxCleanJobImpl;
import ru.romanov.outbox.service.OutboxMessageService;
import ru.romanov.outbox.service.OutboxMessageStatusService;
import ru.romanov.outbox.service.impl.OutboxMessageServiceImpl;
import ru.romanov.outbox.service.impl.OutboxMessageStatusServiceImpl;
import ru.romanov.outbox.service.impl.ScheduledOutboxRecoveryService;
import ru.romanov.outbox.service.impl.StartupOutboxRecoveryService;
import ru.romanov.outbox.storage.OutboxMessageQueue;
import ru.romanov.outbox.storage.impl.OutboxMessageQueueImpl;

import javax.sql.DataSource;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(
            value = {"streaming.virtual-threads-enabled"},
            havingValue = "true",
            matchIfMissing = true)
    public ExecutorService virtualMessagesExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    @ConditionalOnProperty(
            value = {"streaming.virtual-threads-enabled"},
            havingValue = "false",
            matchIfMissing = true)
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
    @ConditionalOnMissingBean
    public MeterRegistry meterRegistry() {
        return new SimpleMeterRegistry();
    }

    @Bean
    public OutboxMetrics outboxMetrics(MeterRegistry registry) {
        return new OutboxMetrics(registry);
    }

    @Bean
    public OutboxMessageStatusService outboxMessageStatusService(OutboxMessageRepository repository,
                                                                 StreamingProperties properties,
                                                                 MeterRegistry meterRegistry,
                                                                 @Qualifier("messagesExecutor") Executor messagesExecutor) {
        return new OutboxMessageStatusServiceImpl(repository, properties, meterRegistry, messagesExecutor);
    }

    @Bean
    @ConditionalOnMissingBean(ScheduledOutboxProcessor.class)
    public AbstractOutboxProcessor eventOrientedOutboxProcessor(OutboxMessageQueue outboxQueue,
                                                                KafkaTemplateFactory kafkaTemplateFactory,
                                                                @Qualifier("messagesExecutor") Executor messagesExecutor,
                                                                StreamingProperties properties,
                                                                ObjectMapper objectMapper, OutboxMetrics outboxMetrics,
                                                                OutboxMessageStatusService statusService) {
        return new EventOrientedOutboxProcessorImpl(outboxQueue, objectMapper, statusService, kafkaTemplateFactory,
                messagesExecutor, properties, outboxMetrics);
    }

    @Bean
    @ConditionalOnProperty(
            value = {"streaming.outbox.scheduled.enabled"},
            havingValue = "true",
            matchIfMissing = true)
    public AbstractOutboxProcessor scheduledOutboxProcessor(OutboxMessageQueue outboxQueue,
                                                            KafkaTemplateFactory kafkaTemplateFactory,
                                                            @Qualifier("messagesExecutor") Executor messagesExecutor,
                                                            StreamingProperties properties, ObjectMapper objectMapper,
                                                            OutboxMetrics outboxMetrics,
                                                            OutboxMessageStatusService statusService,
                                                            OutboxMessageRepository repository) {
        return new ScheduledOutboxProcessor(objectMapper, statusService, kafkaTemplateFactory, messagesExecutor,
                properties, outboxMetrics, repository);
    }

    @Bean
    @ConditionalOnMissingBean(ScheduledOutboxProcessor.class)
    public StartupOutboxRecoveryService startupOutboxRecoveryService(OutboxMessageRepository repository,
                                                                     OutboxMessageQueue queue,
                                                                     StreamingProperties properties) {
        return new StartupOutboxRecoveryService(properties, repository, queue);
    }

    @Bean
    @ConditionalOnMissingBean(ScheduledOutboxProcessor.class)
    public ScheduledOutboxRecoveryService scheduledOutboxRecoveryService(OutboxMessageRepository repository,
                                                                         OutboxMessageQueue queue,
                                                                         StreamingProperties properties) {
        return new ScheduledOutboxRecoveryService(properties, repository, queue);
    }

    @Bean
    public StreamingFacade embeddedStreamingDataProcessor(OutboxMessageService outboxService) {
        return new OutboxStreamingFacade(outboxService);
    }

    @Bean
    public OutboxMessageService outboxMessageService(StreamingProperties streamingProperties,
                                                     OutboxMessageRepository repository, OutboxMessageQueue outboxQueue,
                                                     ObjectMapper objectMapper, OutboxMetrics outboxMetrics) {
        return new OutboxMessageServiceImpl(streamingProperties, repository, outboxQueue, outboxMetrics, objectMapper);
    }


    @Bean
    public OutboxMessageRepository outboxMessageRepository(NamedParameterJdbcTemplate jdbcTemplate,
                                                           StreamingProperties properties) {
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
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public OutboxCleanJob miniOutboxCleanerService(OutboxMessageRepository repository, StreamingProperties properties) {
        return new OutboxCleanJobImpl(repository, properties);
    }
}
