package ru.romanov.outbox.domain.repository.impl;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static ru.romanov.outbox.domain.enums.OutboxMessageStatus.FAILURE;
import static ru.romanov.outbox.domain.enums.OutboxMessageStatus.NEW;

@Repository
@RequiredArgsConstructor
public class OutboxMessageRepositoryImpl implements OutboxMessageRepository {

    private static final String INSERT_SQL = "INSERT INTO %s " + "(id, key, payload, headers, " + "topic, kafka_system, create_time, reserved_to, status) " + "VALUES (:id, :key, :payload::json, :headers::jsonb, :topic, :kafka_system, :create_time, :reserved_to, :status)";

    private static final String UPDATE_SQL = "UPDATE %s " + "SET status = :status, " + "reserved_to = :reservedTo, " + "WHERE id = :id";

    private static final String DELETE_PROCESSED_MESSAGES_SQL = "DELETE FROM %s " + "WHERE id IN(" + "SELECT id FROM %s " + "WHERE create_time < CURRENT_DATE - :daysInterval " + "AND status = 'SUCCESS' LIMIT :limit FOR UPDATE SKIP LOCKED)";

    private static final String SELECT_STUCK_MESSAGES_SQL = "SELECT * FROM %s " + "WHERE status IN (:statuses) " + "AND reserved_to < :now " + "LIMIT :limit " + "FOR UPDATE SKIP LOCKED";

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final StreamingProperties properties;

    @Override
    public OutboxMessageEntity save(OutboxMessageEntity entity) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", entity.getId())
                .addValue("key", entity.getKey())
                .addValue("payload", entity.getPayload())
                .addValue("headers", entity.getHeaders())
                .addValue("topic", entity.getTopic())
                .addValue("kafka_system", entity.getKafkaSystem())
                .addValue("create_time", entity.getCreateTime())
                .addValue("reserved_to", entity.getReservedTo())
                .addValue("status", entity.getStatus().name());

        jdbcTemplate.update(String.format(INSERT_SQL, properties.getOutbox().getTableName()), params);
        return entity;
    }

    @Override
    public void updateState(OutboxMessageEntity entity) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("id", entity.getId())
                .addValue("reservedTo", entity.getReservedTo())
                .addValue("status", entity.getStatus().name());

        jdbcTemplate.update(String.format(UPDATE_SQL, properties.getOutbox().getTableName()), params);
    }

    @Override
    public void deleteProcessedMessages(Integer daysInterval, Integer limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("daysInterval", daysInterval)
                .addValue("limit", limit);
        var tableName = properties.getOutbox().getTableName();
        jdbcTemplate.update(String.format(DELETE_PROCESSED_MESSAGES_SQL, tableName, tableName), params);
    }

    @Override
    public List<OutboxMessageEntity> findStuckMessages(Integer limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue("now", LocalDateTime.now())
                .addValue("limit", limit)
                .addValue("statuses", List.of(NEW.name(), FAILURE.name()));

        return jdbcTemplate.query(String.format(SELECT_STUCK_MESSAGES_SQL, properties.getOutbox()
                .getTableName()), params, (rs, rowNum) -> OutboxMessageEntity.builder()
                .id(rs.getObject("id", UUID.class))
                .key(rs.getString("key"))
                .payload(rs.getObject("payload", JsonNode.class))
                .headers(rs.getObject("headers", JsonNode.class))
                .topic(rs.getString("topic"))
                .kafkaSystem(rs.getString("kafka_system"))
                .createTime(rs.getTimestamp("create_time").toLocalDateTime())
                .reservedTo(rs.getTimestamp("reserved_to").toLocalDateTime())
                .status(OutboxMessageStatus.valueOf(rs.getString("status")))
                .build());
    }
}
