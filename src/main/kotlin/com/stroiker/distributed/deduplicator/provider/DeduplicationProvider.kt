package com.stroiker.distributed.deduplicator.provider

import com.datastax.oss.driver.api.core.ConsistencyLevel.EACH_QUORUM
import com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable
import com.stroiker.distributed.deduplicator.absorber.DuplicateBurstAbsorber
import com.stroiker.distributed.deduplicator.exception.DuplicateException
import com.stroiker.distributed.deduplicator.exception.FailedException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.DUPLICATE
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.FAILED
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.RETRY
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.SUCCESS
import com.stroiker.distributed.deduplicator.strategy.sync.RetryStrategy
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

open class DeduplicationProvider internal constructor(
    private val session: CqlSession,
    private val profileName: String,
    private val absorber: DuplicateBurstAbsorber,
    private val strategy: RetryStrategy
) {

    private val preparedStatementCache = ConcurrentHashMap<String, PreparedStatement>()

    open fun <T> process(
        key: String,
        table: String,
        keyspace: String,
        ttl: Duration,
        block: () -> T
    ): T = strategy.retry {
        runCatching {
            UUID.randomUUID().toString().let { selfRecordUuid ->
                absorber.absorb("$keyspace:$table:$key") {
                    insertRecord(
                        key = key,
                        keyspace = keyspace,
                        recordUuid = selfRecordUuid,
                        table = table,
                        ttl = ttl,
                        state = SUCCESS
                    )
                    selfRecordUuid
                }.also { absorbedRecordUuid ->
                    if (absorbedRecordUuid != selfRecordUuid) {
                        insertRecord(
                            key = key,
                            keyspace = keyspace,
                            recordUuid = selfRecordUuid,
                            table = table,
                            ttl = ttl,
                            state = DUPLICATE
                        )
                        throw DuplicateException(key, table)
                    }
                }
                val successRecords = getSuccessRecords(key = key, table = table, keyspace = keyspace)
                if (successRecords.size > 1) {
                    val winner = successRecords.first()
                    if (selfRecordUuid == winner.recordUuid) {
                        updateRecordState(
                            key = key,
                            table = table,
                            keyspace = keyspace,
                            timeUuid = winner.timeUuid,
                            recordUuid = winner.recordUuid,
                            state = RETRY,
                            ttl = ttl
                        )
                        throw RetryException(key, table)
                    } else {
                        successRecords.first { record -> record.recordUuid == selfRecordUuid }.let { self ->
                            updateRecordState(
                                key = key,
                                table = table,
                                keyspace = keyspace,
                                timeUuid = self.timeUuid,
                                recordUuid = self.recordUuid,
                                state = DUPLICATE,
                                ttl = ttl
                            )
                        }
                        throw DuplicateException(key, table)
                    }
                } else {
                    kotlin.runCatching { block.invoke() }.getOrElse { error ->
                        runCatching {
                            requireNotNull(successRecords.first { it.recordUuid == selfRecordUuid }) { "self record must not be null" }.let { self ->
                                updateRecordState(
                                    key = key,
                                    table = table,
                                    keyspace = keyspace,
                                    timeUuid = self.timeUuid,
                                    recordUuid = self.recordUuid,
                                    state = FAILED,
                                    ttl = ttl
                                )
                            }
                        }.getOrElse { updateError ->
                            updateError.addSuppressed(error)
                            throw updateError
                        }
                        throw error
                    }
                }
            }
        }.getOrElse { error ->
            if (error is FailedException) {
                absorber.evict("$keyspace:$table:$key")
            }
            throw error
        }
    }

    private fun getSuccessRecords(key: String, table: String, keyspace: String): List<DeduplicationData> =
        getSelectStatement(table = table, keyspace = keyspace).bind()
            .setString(KEY_COLUMN, key)
            .let { boundStatement ->
                runCatching { session.execute(boundStatement) }
                    .getOrElse { error -> throw FailedException(key, table, error.message) }
                    .map { row -> row.toDeduplicationData() }
                    .filter { deduplicationData -> deduplicationData.recordState == SUCCESS }
            }

    private fun insertRecord(
        key: String,
        table: String,
        keyspace: String,
        recordUuid: String,
        state: RecordState,
        ttl: Duration
    ) {
        getInsertStatement(table = table, keyspace = keyspace).bind()
            .setString(KEY_COLUMN, key)
            .setString(RECORD_UUID_COLUMN, recordUuid)
            .setShort(STATE_COLUMN, state.value)
            .setInt(TTL, ttl.seconds.toInt())
            .also { boundStatement ->
                runCatching { session.execute(boundStatement) }
                    .getOrElse { error -> throw FailedException(key, table, error.message) }
                    .wasApplied().also { applied ->
                        if (!applied) throw FailedException(key, table, "Insert record wasn't applied")
                    }
            }
    }

    private fun updateRecordState(
        key: String,
        table: String,
        keyspace: String,
        timeUuid: UUID,
        recordUuid: String,
        state: RecordState,
        ttl: Duration
    ) {
        getUpsertStatement(table = table, keyspace = keyspace).bind()
            .setString(KEY_COLUMN, key)
            .setUuid(TIME_UUID_COLUMN, timeUuid)
            .setString(RECORD_UUID_COLUMN, recordUuid)
            .setShort(STATE_COLUMN, state.value)
            .setInt(TTL, ttl.seconds.toInt())
            .also { boundStatement ->
                runCatching { session.execute(boundStatement) }
                    .getOrElse { error -> throw FailedException(key, table, error.message) }
                    .wasApplied().also { applied ->
                        if (!applied) throw FailedException(key, table, "Update record to '$state' wasn't applied")
                    }
            }
    }

    private fun getSelectStatement(table: String, keyspace: String): PreparedStatement =
        preparedStatementCache.computeIfAbsent("s:$keyspace:$table") {
            createTableIfNotExist(table = table, keyspace = keyspace)
            session.prepare(
                QueryBuilder.selectFrom(keyspace, table)
                    .columns(TIME_UUID_COLUMN, RECORD_UUID_COLUMN, STATE_COLUMN)
                    .whereColumn(KEY_COLUMN).isEqualTo(QueryBuilder.bindMarker(KEY_COLUMN))
                    .build()
                    .setExecutionProfileName(profileName)
                    .setConsistencyLevel(EACH_QUORUM)
            )
        }

    private fun getInsertStatement(table: String, keyspace: String): PreparedStatement =
        preparedStatementCache.computeIfAbsent("i:$keyspace:$table") {
            createTableIfNotExist(table = table, keyspace = keyspace)
            session.prepare(
                QueryBuilder.insertInto(keyspace, table)
                    .value(KEY_COLUMN, QueryBuilder.bindMarker(KEY_COLUMN))
                    .value(TIME_UUID_COLUMN, QueryBuilder.now())
                    .value(RECORD_UUID_COLUMN, QueryBuilder.bindMarker(RECORD_UUID_COLUMN))
                    .value(STATE_COLUMN, QueryBuilder.bindMarker(STATE_COLUMN))
                    .usingTtl(QueryBuilder.bindMarker(TTL))
                    .build()
                    .setExecutionProfileName(profileName)
                    .setConsistencyLevel(LOCAL_QUORUM)
            )
        }

    private fun getUpsertStatement(table: String, keyspace: String): PreparedStatement =
        preparedStatementCache.computeIfAbsent("u:$keyspace:$table") {
            createTableIfNotExist(table = table, keyspace = keyspace)
            session.prepare(
                QueryBuilder.insertInto(keyspace, table)
                    .value(KEY_COLUMN, QueryBuilder.bindMarker(KEY_COLUMN))
                    .value(TIME_UUID_COLUMN, QueryBuilder.bindMarker(TIME_UUID_COLUMN))
                    .value(RECORD_UUID_COLUMN, QueryBuilder.bindMarker(RECORD_UUID_COLUMN))
                    .value(STATE_COLUMN, QueryBuilder.bindMarker(STATE_COLUMN))
                    .usingTtl(QueryBuilder.bindMarker(TTL))
                    .build()
                    .setExecutionProfileName(profileName)
                    .setConsistencyLevel(LOCAL_QUORUM)
            )
        }

    private fun createTableIfNotExist(table: String, keyspace: String) {
        createTable(keyspace, table)
            .ifNotExists()
            .withPartitionKey(KEY_COLUMN, DataTypes.TEXT)
            .withClusteringColumn(TIME_UUID_COLUMN, DataTypes.TIMEUUID)
            .withClusteringColumn(RECORD_UUID_COLUMN, DataTypes.TEXT)
            .withColumn(STATE_COLUMN, DataTypes.SMALLINT)
            .withClusteringOrder(TIME_UUID_COLUMN, ClusteringOrder.ASC)
            .build()
            .also { session.execute(it) }
    }

    private class DeduplicationData(
        val timeUuid: UUID,
        val recordUuid: String,
        val recordState: RecordState
    )

    private fun Row.toDeduplicationData() = DeduplicationData(
        recordUuid = get(RECORD_UUID_COLUMN, TypeCodecs.TEXT)!!,
        timeUuid = get(TIME_UUID_COLUMN, TypeCodecs.TIMEUUID)!!,
        recordState = RecordState.of(get(STATE_COLUMN, TypeCodecs.SMALLINT)!!)
    )

    enum class RecordState(val value: Short) {
        SUCCESS(1), DUPLICATE(2), RETRY(3), FAILED(4);

        companion object {
            fun of(value: Short): RecordState = RecordState.values().find { it.value == value }
                ?: throw IllegalArgumentException("Unsupported state '$value'")
        }
    }

    companion object {
        internal const val KEY_COLUMN = "key"
        internal const val TIME_UUID_COLUMN = "time_uuid"
        internal const val RECORD_UUID_COLUMN = "record_uuid"
        internal const val STATE_COLUMN = "state"
        internal const val TTL = "ttl"
    }
}
