package com.stroiker.distributed.deduplicator.provider

import com.datastax.oss.driver.api.core.ConsistencyLevel.EACH_QUORUM
import com.datastax.oss.driver.api.core.ConsistencyLevel.LOCAL_QUORUM
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile
import com.datastax.oss.driver.api.core.cql.PreparedStatement
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.oss.driver.api.core.type.DataTypes
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable
import com.datastax.oss.driver.api.querybuilder.relation.Relation
import com.stroiker.distributed.deduplicator.exception.DuplicateException
import com.stroiker.distributed.deduplicator.exception.FailedException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.DUPLICATE
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.FAILED
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.RETRY
import com.stroiker.distributed.deduplicator.provider.DeduplicationProvider.RecordState.SUCCESS
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import com.stroiker.distributed.deduplicator.strategy.impl.FixedDelayRetryStrategy
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class DeduplicationProvider private constructor(
    private val session: CqlSession,
    private val profileName: String,
    private val retryStrategy: RetryStrategy
) {

    private val preparedStatementCache = ConcurrentHashMap<String, PreparedStatement>()

    init {
        when (session.context.configLoader.initialConfig.getProfile(profileName).getString(DefaultDriverOption.REQUEST_CONSISTENCY)) {
            LOCAL_QUORUM.name(), EACH_QUORUM.name() -> {}
            else -> throw UnsupportedOperationException("Only LOCAL_QUORUM or EACH_QUORUM consistency levels are supported. Weaker consistency levels can not guarantee strict deduplication")
        }
    }

    fun <T> process(
        key: String,
        table: String,
        keyspace: String,
        ttl: Duration,
        block: () -> T
    ): T = UUID.randomUUID().toString().let { selfRecordUuid ->
        retryStrategy.retry {
            insertRecord(key = key, keyspace = keyspace, recordUuid = selfRecordUuid, table = table, ttl = ttl)
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
    }

    private fun getSuccessRecords(key: String, table: String, keyspace: String): List<DeduplicationData> =
        getSelectStatement(table = table, keyspace = keyspace).bind().setString(KEY_COLUMN, key).let { boundStatement ->
            session.execute(boundStatement).map { row -> row.toDeduplicationData() }
                .filter { deduplicationData -> deduplicationData.recordState == SUCCESS }
        }

    private fun insertRecord(
        key: String,
        table: String,
        keyspace: String,
        recordUuid: String,
        ttl: Duration
    ) {
        getInsertStatement(table = table, keyspace = keyspace).bind().setString(KEY_COLUMN, key)
            .setString(STATE_COLUMN, SUCCESS.name)
            .setString(RECORD_UUID_COLUMN, recordUuid).setInt(TTL, ttl.seconds.toInt()).also { boundStatement ->
                session.execute(boundStatement).wasApplied().also { applied ->
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
        getUpdateStatement(table = table, keyspace = keyspace).bind()
            .setString(KEY_COLUMN, key)
            .setUuid(TIME_UUID_COLUMN, timeUuid)
            .setString(RECORD_UUID_COLUMN, recordUuid).setString(STATE_COLUMN, state.name)
            .setInt(TTL, ttl.seconds.toInt()).also { boundStatement ->
                session.execute(boundStatement).wasApplied().also { applied ->
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
            )
        }

    private fun getUpdateStatement(table: String, keyspace: String): PreparedStatement =
        preparedStatementCache.computeIfAbsent("u:$keyspace:$table") {
            createTableIfNotExist(table = table, keyspace = keyspace)
            session.prepare(
                QueryBuilder.update(keyspace, table)
                    .usingTtl(QueryBuilder.bindMarker(TTL))
                    .setColumn(STATE_COLUMN, QueryBuilder.bindMarker(STATE_COLUMN))
                    .where(
                        Relation.column(KEY_COLUMN).isEqualTo(QueryBuilder.bindMarker(KEY_COLUMN)),
                        Relation.column(TIME_UUID_COLUMN).isEqualTo(QueryBuilder.bindMarker(TIME_UUID_COLUMN)),
                        Relation.column(RECORD_UUID_COLUMN).isEqualTo(QueryBuilder.bindMarker(RECORD_UUID_COLUMN))
                    )
                    .build()
                    .setExecutionProfileName(profileName)
            )
        }

    private fun createTableIfNotExist(table: String, keyspace: String) {
        createTable(keyspace, table)
            .ifNotExists()
            .withPartitionKey(KEY_COLUMN, DataTypes.TEXT)
            .withClusteringColumn(TIME_UUID_COLUMN, DataTypes.TIMEUUID)
            .withClusteringColumn(RECORD_UUID_COLUMN, DataTypes.TEXT)
            .withColumn(STATE_COLUMN, DataTypes.TEXT)
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
        recordState = RecordState.valueOf(get(STATE_COLUMN, TypeCodecs.TEXT)!!)
    )

    enum class RecordState {
        SUCCESS, FAILED, DUPLICATE, RETRY
    }

    class DeduplicationProviderBuilder {

        private var session: Lazy<CqlSession> = lazy {
            CqlSession.builder()
                .withConfigLoader(DriverConfigLoader.fromClasspath("application.conf"))
                .build()
        }
        private var strategy: RetryStrategy = FixedDelayRetryStrategy(3, Duration.ofMillis(100))
        private var profileName: String = DriverExecutionProfile.DEFAULT_NAME

        fun session(session: CqlSession): DeduplicationProviderBuilder {
            this.session = lazyOf(session)
            return this
        }

        fun strategy(strategy: RetryStrategy): DeduplicationProviderBuilder {
            this.strategy = strategy
            return this
        }

        fun profile(profileName: String): DeduplicationProviderBuilder {
            this.profileName = profileName
            return this
        }

        fun build(): DeduplicationProvider = DeduplicationProvider(session.value, profileName, strategy)
    }

    companion object {
        internal const val KEY_COLUMN = "key"
        internal const val TIME_UUID_COLUMN = "time_uuid"
        internal const val RECORD_UUID_COLUMN = "record_uuid"
        internal const val STATE_COLUMN = "state"
        internal const val TTL = "ttl"

        fun builder() = DeduplicationProviderBuilder()
    }
}
