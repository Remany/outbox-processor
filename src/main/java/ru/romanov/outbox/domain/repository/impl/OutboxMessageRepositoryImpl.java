package ru.romanov.outbox.domain.repository.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import ru.romanov.outbox.configuration.StreamingProperties;
import ru.romanov.outbox.domain.enums.OutboxMessageStatus;
import ru.romanov.outbox.domain.model.OutboxMessageEntity;
import ru.romanov.outbox.domain.repository.OutboxMessageRepository;
import ru.romanov.outbox.storage.UpdateEntry;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.romanov.outbox.domain.enums.OutboxMessageStatus.FAILURE;
import static ru.romanov.outbox.domain.enums.OutboxMessageStatus.NEW;

@Repository
@RequiredArgsConstructor
public class OutboxMessageRepositoryImpl implements OutboxMessageRepository {

    private static final String INSERT_SQL = "INSERT INTO %s " + "(id, key, payload, headers, " + "topic, kafka_system, create_time, update_time, reserved_to, status) " + "VALUES (:id, :key, :payload::json, :headers::jsonb, :topic, :kafka_system, :create_time, :update_time, :reserved_to, :status)";

    private static final String UPDATE_STATUS_BATCH_SQL = "UPDATE %s SET status = :status, update_time = :update_time WHERE id IN (:ids)";

    private static final String DELETE_PROCESSED_MESSAGES_SQL = "DELETE FROM %s " + "WHERE id IN(" + "SELECT id FROM %s " + "WHERE create_time < CURRENT_DATE - :daysInterval " + "AND status = 'SUCCESS' LIMIT :limit FOR UPDATE SKIP LOCKED)";

    private static final String SELECT_NEW_MESSAGES_SQL = "SELECT * FROM %s " + "WHERE status IN (:statuses) " + "LIMIT :limit " + "FOR UPDATE SKIP LOCKED";

    private static final String SELECT_STUCK_MESSAGES_SQL = "SELECT * FROM %s " + "WHERE status IN (:statuses) " + "AND reserved_to < :reserved_to " + "LIMIT :limit " + "FOR UPDATE SKIP LOCKED";

    private static final String ID_PARAM = "id";
    private static final String IDS_PARAM = "ids";
    private static final String KEY_PARAM = "key";
    private static final String PAYLOAD_PARAM = "payload";
    private static final String HEADERS_PARAM = "headers";
    private static final String TOPIC_PARAM = "topic";
    private static final String KAFKA_SYSTEM_PARAM = "kafka_system";
    private static final String CREATE_TIME_PARAM = "create_time";
    private static final String UPDATE_TIME_PARAM = "update_time";
    private static final String RESERVED_TO_PARAM = "reserved_to";
    private static final String STATUS_PARAM = "status";
    private static final String STATUSES_PARAM = "statuses";
    private static final String LIMIT_PARAM = "limit";
    private static final String DAYS_INTERVAL_PARAM = "daysInterval";

    private static final List<String> STUCK_STATUSES = List.of(NEW.name(), FAILURE.name());

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final StreamingProperties properties;

    @Override
    public OutboxMessageEntity save(OutboxMessageEntity entity) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue(ID_PARAM, entity.getId())
                .addValue(KEY_PARAM, entity.getKey())
                .addValue(PAYLOAD_PARAM, entity.getPayload())
                .addValue(HEADERS_PARAM, entity.getHeaders())
                .addValue(TOPIC_PARAM, entity.getTopic())
                .addValue(KAFKA_SYSTEM_PARAM, entity.getKafkaSystem())
                .addValue(CREATE_TIME_PARAM, entity.getCreateTime())
                .addValue(UPDATE_TIME_PARAM, entity.getUpdateTime())
                .addValue(RESERVED_TO_PARAM, entity.getReservedTo())
                .addValue(STATUS_PARAM, entity.getStatus().name());

        jdbcTemplate.update(String.format(INSERT_SQL, properties.getOutbox().getTableName()), params);
        return entity;
    }

    @Override
    public void updateStatuses(List<UpdateEntry> batch) {
        if (batch.isEmpty()) {
            return;
        }
        batch.stream().collect(Collectors.groupingBy(
                UpdateEntry::status,
                Collectors.mapping(e -> e.ref().id(), Collectors.toList())
        )).forEach((status, ids) -> {
            MapSqlParameterSource params = new MapSqlParameterSource().addValue(IDS_PARAM, ids)
                    .addValue(STATUS_PARAM, status.name()).addValue(UPDATE_TIME_PARAM, LocalDateTime.now());
            var tableName = properties.getOutbox().getTableName();
            jdbcTemplate.update(String.format(UPDATE_STATUS_BATCH_SQL, tableName), params);
        });
    }

    @Override
    public void deleteProcessedMessages(Integer daysInterval, Integer limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue(DAYS_INTERVAL_PARAM, daysInterval)
                .addValue(LIMIT_PARAM, limit);
        var tableName = properties.getOutbox().getTableName();
        jdbcTemplate.update(String.format(DELETE_PROCESSED_MESSAGES_SQL, tableName, tableName), params);
    }

    @Override
    public List<OutboxMessageEntity> findNewMessages(Integer limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue(LIMIT_PARAM, limit)
                .addValue(STATUSES_PARAM, STUCK_STATUSES);

        return findList(SELECT_NEW_MESSAGES_SQL, params);
    }

    @Override
    public List<OutboxMessageEntity> findStuckMessages(Integer limit) {
        MapSqlParameterSource params = new MapSqlParameterSource().addValue(RESERVED_TO_PARAM, LocalDateTime.now())
                .addValue(LIMIT_PARAM, limit)
                .addValue(STATUSES_PARAM, STUCK_STATUSES);

        return findList(SELECT_STUCK_MESSAGES_SQL, params);
    }

    private List<OutboxMessageEntity> findList(String sqlQuery, MapSqlParameterSource params) {
        return jdbcTemplate.query(String.format(sqlQuery, properties.getOutbox()
                .getTableName()), params, (rs, rowNum) -> OutboxMessageEntity.builder()
                .id(rs.getObject(ID_PARAM, UUID.class))
                .key(rs.getString(KEY_PARAM))
                .payload(rs.getString(PAYLOAD_PARAM))
                .headers(rs.getString(HEADERS_PARAM))
                .topic(rs.getString(TOPIC_PARAM))
                .kafkaSystem(rs.getString(KAFKA_SYSTEM_PARAM))
                .createTime(rs.getTimestamp(CREATE_TIME_PARAM).toLocalDateTime())
                .reservedTo(rs.getTimestamp(RESERVED_TO_PARAM).toLocalDateTime())
                .status(OutboxMessageStatus.valueOf(rs.getString(STATUS_PARAM)))
                .build());
    }
}
