package com.stroiker.distributed.deduplicator.provider

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.DefaultDriverOption
import com.datastax.oss.driver.api.core.config.DriverConfigLoader
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.cql.Statement
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace
import com.nhaarman.mockitokotlin2.argThat
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.whenever
import com.stroiker.distributed.deduplicator.exception.DuplicateException
import com.stroiker.distributed.deduplicator.exception.FailedException
import com.stroiker.distributed.deduplicator.exception.RetryException
import com.stroiker.distributed.deduplicator.randomString
import com.stroiker.distributed.deduplicator.strategy.RetryStrategy
import com.stroiker.distributed.deduplicator.strategy.impl.NoRetryStrategy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junitpioneer.jupiter.RetryingTest
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

class DeduplicationProviderTest {

    private val keyspace = "test_keyspace"
    private val table = "test_table"
    private val profileName = "test-profile"

    @Container
    val cassandraCnt = CassandraContainer("cassandra").apply { start() }

    private val session = spy(
        CqlSession.builder()
            .addContactPoints(listOf(InetSocketAddress("localhost", cassandraCnt.firstMappedPort)))
            .withLocalDatacenter("datacenter1")
            .withConfigLoader(
                DriverConfigLoader.programmaticBuilder()
                    .startProfile(profileName)
                    .withString(DefaultDriverOption.REQUEST_CONSISTENCY, ConsistencyLevel.LOCAL_QUORUM.name())
                    .endProfile()
                    .build()
            )
            .build()
    )

    private val provider = DeduplicationProvider.builder()
        .session(session)
        .profile(profileName)
        .strategy(NoRetryStrategy())
        .build()

    @BeforeEach
    fun init() {
        createKeyspace(keyspace)
            .ifNotExists()
            .withSimpleStrategy(1)
            .build()
            .also { session.execute(it) }
    }

    @Test
    fun `process one key - success`() {
        val key = randomString()

        provider.process(key = key, table = table, keyspace = keyspace, ttl = Duration.ZERO) { 42 }.also { result ->
            assertEquals(42, result)
        }
        session.execute(
            QueryBuilder.selectFrom(keyspace, table)
                .all()
                .whereColumn(DeduplicationProvider.KEY_COLUMN)
                .isEqualTo(QueryBuilder.literal(key, TypeCodecs.TEXT))
                .orderBy(DeduplicationProvider.TIME_UUID_COLUMN, ClusteringOrder.ASC)
                .build()
        ).all().also { rows ->
            assertEquals(1, rows.size)
            rows.first().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.SUCCESS.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
        }
    }

    @Test
    fun `process multiple duplicate keys sequentially - success`() {
        val key = randomString()

        provider.process(key = key, table = table, keyspace = keyspace, ttl = Duration.ZERO) { 42 }.also { result ->
            assertEquals(42, result)
        }
        assertThrows<DuplicateException> {
            provider.process(
                key = key,
                table = table,
                keyspace = keyspace,
                ttl = Duration.ZERO
            ) { 42 }
        }
        session.execute(
            QueryBuilder.selectFrom(keyspace, table)
                .all()
                .whereColumn(DeduplicationProvider.KEY_COLUMN)
                .isEqualTo(QueryBuilder.literal(key, TypeCodecs.TEXT))
                .orderBy(DeduplicationProvider.TIME_UUID_COLUMN, ClusteringOrder.ASC)
                .build()
        ).all().also { rows ->
            assertEquals(2, rows.size)
            rows.first().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.SUCCESS.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
            rows.last().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.DUPLICATE.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
        }
    }

    @RetryingTest(maxAttempts = 5, name = "should process multiple duplicate keys in parallel - success")
    fun `should process multiple duplicate keys in parallel - success`() {
        val cdl = CountDownLatch(1)
        val provider = DeduplicationProvider.builder()
            .session(session)
            .profile(profileName)
            .strategy(
                object : RetryStrategy {
                    override fun <T> retry(action: () -> T): T {
                        cdl.await()
                        return action()
                    }
                }
            )
            .build()
        val key = randomString()
        val futures = mutableListOf<Future<Void>>()
        futures.add(
            CompletableFuture.runAsync {
                provider.process(
                    key = key,
                    table = table,
                    keyspace = keyspace,
                    ttl = Duration.ZERO
                ) { 42 }
            }
        )
        futures.add(
            CompletableFuture.runAsync {
                provider.process(
                    key = key,
                    table = table,
                    keyspace = keyspace,
                    ttl = Duration.ZERO
                ) { 42 }
            }
        )
        cdl.countDown()
        assertThrows<ExecutionException> { futures.forEach { future -> future.get() } }.also { error ->
            assertTrue(error.cause is RetryException)
        }
        session.execute(
            QueryBuilder.selectFrom(keyspace, table)
                .all()
                .whereColumn(DeduplicationProvider.KEY_COLUMN)
                .isEqualTo(QueryBuilder.literal(key, TypeCodecs.TEXT))
                .orderBy(DeduplicationProvider.TIME_UUID_COLUMN, ClusteringOrder.ASC)
                .build()
        ).all().also { rows ->
            assertEquals(2, rows.size)
            rows.first().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.RETRY.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
            rows.last().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.DUPLICATE.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
        }
    }

    @Test
    fun `should process one key - execution error`() {
        val key = randomString()

        assertThrows<IllegalArgumentException> {
            provider.process(
                key = key,
                table = table,
                keyspace = keyspace,
                ttl = Duration.ZERO
            ) { throw IllegalArgumentException() }
        }
        session.execute(
            QueryBuilder.selectFrom(keyspace, table)
                .all()
                .whereColumn(DeduplicationProvider.KEY_COLUMN)
                .isEqualTo(QueryBuilder.literal(key, TypeCodecs.TEXT))
                .orderBy(DeduplicationProvider.TIME_UUID_COLUMN, ClusteringOrder.ASC)
                .build()
        ).all().also { rows ->
            assertEquals(1, rows.size)
            rows.first().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.FAILED.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
        }
    }

    @Test
    fun `should process one key - error when processing and change state wasn't applied`() {
        val key = randomString()
        val mockResultSet: ResultSet = mock()
        whenever(mockResultSet.wasApplied()).thenReturn(false)
        doReturn(mockResultSet).`when`(session).execute(
            argThat<Statement<*>> {
                when (this) {
                    is BoundStatement -> this.preparedStatement.query.contains("INSERT", true) && this.getString(DeduplicationProvider.STATE_COLUMN) == DeduplicationProvider.RecordState.FAILED.name
                    else -> false
                }
            }
        )
        assertThrows<FailedException> {
            provider.process(
                key = key,
                table = table,
                keyspace = keyspace,
                ttl = Duration.ZERO
            ) { throw IllegalArgumentException() }
        }.also { error ->
            assertTrue(error.suppressed.first() is IllegalArgumentException)
        }
        session.execute(
            QueryBuilder.selectFrom(keyspace, table)
                .all()
                .whereColumn(DeduplicationProvider.KEY_COLUMN)
                .isEqualTo(QueryBuilder.literal(key, TypeCodecs.TEXT))
                .orderBy(DeduplicationProvider.TIME_UUID_COLUMN, ClusteringOrder.ASC)
                .build()
        ).all().also { rows ->
            assertEquals(1, rows.size)
            rows.first().also { row ->
                assertEquals(key, row.get(DeduplicationProvider.KEY_COLUMN, TypeCodecs.TEXT))
                assertEquals(
                    DeduplicationProvider.RecordState.SUCCESS.name,
                    row.get(DeduplicationProvider.STATE_COLUMN, TypeCodecs.TEXT)
                )
            }
        }
    }

    class CassandraContainer(imageName: String) : GenericContainer<CassandraContainer>(imageName) {
        init {
            withExposedPorts(9042)
            withEnv("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch")
            withEnv("JVM_OPTS", "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0")
            withEnv("HEAP_NEWSIZE", "128M")
            withEnv("MAX_HEAP_SIZE", "1024M")
        }
    }
}
