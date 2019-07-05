package com.adrienben.demo.kstreamconnections

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer

internal const val DNS_TOPIC = "dns"
internal const val CONNECTIONS_TOPIC = "connections"
internal const val CONNECTIONS_BY_IP_REKEY_TOPIC = "connections_by_ip_rekey"
internal const val CONNECTIONS_BY_NAME_REKEY_TOPIC = "connections_by_name_rekey"
internal const val DNS_STORE = "dns_store"
internal const val SITES_AGGREGATIONS_STORE = "site_aggregations"
internal const val SITES_TOPIC = "sites"

@Configuration
@EnableKafkaStreams
class StreamConfig @Autowired constructor(
        private val mapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val stringSerde = Serdes.String()
    private val stringConsumed = Consumed.with(stringSerde, stringSerde)
    private val serverSerde = jsonSerde(Server::class.java)
    private val serverProduced = Produced.with(stringSerde, serverSerde)
    private val connectionSerde = jsonSerde(Connection::class.java)
    private val connectionConsumed = Consumed.with(stringSerde, connectionSerde)
    private val connectionProduced = Produced.with(stringSerde, connectionSerde)

    @Bean
    fun kStream(streamBuilder: StreamsBuilder): KStream<String, Server> {

        // Here we consumed the topic containing the hostname to ip mapping as a KTable.
        val dnsTable = streamBuilder.table(DNS_TOPIC, stringConsumed,
                materializedAsPersistentStore(DNS_STORE, stringSerde, stringSerde))

        // Here we consumed the connection topic and split it in 3 streams:
        // - The first contains the connection by ip address
        // - The second contains the connection by hostname
        // - The last contains the invalid connections
        // The keys of these messages are ignored.
        val connections = streamBuilder
                .stream(CONNECTIONS_TOPIC, connectionConsumed)
                .branch(
                        Predicate { _, c -> c.serverIp != null },
                        Predicate { _, c -> c.serverIp == null && c.serverName != null },
                        Predicate { _, c -> c.serverIp == null && c.serverName == null }
                )

        // Connections by ip.
        // These are just re-keyed and sent to a topic that will contain
        // all connections with the server ip as their key.
        connections[0]
                .selectKey { _, v -> v.serverIp }
                .to(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionProduced)

        // Connection with no Ip but a name.
        // Server ip must be resolved by name. It's done by:
        // - Re-keying the message by server name
        // - Sending them through a topic to ensure the messages are properly partitioned by server name
        // - Then we join the re-keyed message to the dns KTable and inject the server name in the connection message
        // - Then message is re-keyed again (this time by server ip) and sent to the same topic as messages from the previous branch
        connections[1]
                .selectKey { _, v -> v.serverName }
                // The through operation is not mandatory here, just the fact that we combine selectKey and a join operation
                // would trigger a re-keying of the message. The downside of not being explicit here is that the
                // topic name would be generated and might change as topology evolves.
                .through(CONNECTIONS_BY_NAME_REKEY_TOPIC, connectionProduced)
                .leftJoin(
                        dnsTable,
                        { c, mapping -> c.apply { this.serverIp = mapping } },
                        Joined.with(stringSerde, connectionSerde, stringSerde)
                )
                .selectKey { _, v -> v.serverIp }
                .to(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionProduced)

        // Invalid connections (no ip nor name).
        // We just log and error for each of them.
        connections[2]
                .peek { k, v -> logger.error("Received invalid connections: $k - $v") }

        // Now we consume the connections by server ip topic and aggregate the connections events by server:
        // - First we group all messages by key.
        // - Then we aggregate them by setting the correct ip and adding a ConnectionEvent in the aggregated message.
        // - Finally we produce these aggregated in a final topic.
        return streamBuilder
                .stream(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionConsumed)
                .groupByKey()
                .aggregate(
                        { Server() },
                        { k, c, s ->
                            s.apply {
                                this.ip = k
                                this.sourceIps.add(ConnectionEvent(c.sourceIp))
                            }
                        },
                        materializedAsPersistentStore(SITES_AGGREGATIONS_STORE, stringSerde, serverSerde)
                )
                .toStream()
                .apply { this.to(SITES_TOPIC, serverProduced) }
    }

    private fun <T> jsonSerde(clazz: Class<T>) =
            Serdes.serdeFrom(JsonSerializer(mapper), JsonDeserializer(clazz, mapper, false))

    private fun <K, V> materializedAsPersistentStore(storeName: String, keySerde: Serde<K>, valueSerde: Serde<V>) =
            Materialized.`as`<K, V>(Stores.persistentKeyValueStore(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde)
}

/**
 * ObjectMapper configuration.
 */
@Configuration
class JacksonConfiguration {

    /**
     * Exposes an ObjectMapper bean that can de/serialize LocalDateTime, and
     * does not serialize null fields.
     */
    @Bean
    fun objectMapper() = ObjectMapper().apply {
        this.setSerializationInclusion(JsonInclude.Include.NON_NULL)
        this.registerModule(JavaTimeModule())
        this.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        this.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }
}
