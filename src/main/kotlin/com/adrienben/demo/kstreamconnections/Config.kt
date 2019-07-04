package com.adrienben.demo.kstreamconnections

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
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

private const val DNS_TOPIC = "dns"
private const val CONNECTIONS_TOPIC = "connections"
private const val CONNECTIONS_REKEY_TOPIC = "connections_rekey"
private const val SITES_AGGREGATIONS_TOPIC = "site_aggregations"
private const val SITES_TOPIC = "sites"

@Configuration
@EnableKafkaStreams
class StreamConfig @Autowired constructor(
        private val mapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val stringSerde = Serdes.String()
    private val serverSerde = jsonSerde(Server::class.java)
    private val connectionSerde = jsonSerde(Connection::class.java)

    @Bean
    fun kStream(streamBuilder: StreamsBuilder): KStream<String, Server> {
        val dnsTable = streamBuilder.table<String, String>(DNS_TOPIC, Consumed.with(stringSerde, stringSerde))

        val connections = streamBuilder
                .stream<String, Connection>(CONNECTIONS_TOPIC, Consumed.with(stringSerde, connectionSerde))
                .branch(
                        Predicate { _, c -> c.serverIp != null },
                        Predicate { _, c -> c.serverIp == null && c.serverName != null },
                        Predicate { _, c -> c.serverIp == null && c.serverName == null }
                )

        // Connection with an ip
        connections[0]
                .selectKey { _, v -> v.serverIp }
                .to(CONNECTIONS_REKEY_TOPIC, Produced.with(stringSerde, connectionSerde))

        // Connection with no Ip but a name. Ip must be resolved.
        connections[1]
                .selectKey { _, v -> v.serverName }
                .join(
                        dnsTable,
                        { c, mapping -> c.apply { this.serverIp = mapping } },
                        Joined.with(stringSerde, connectionSerde, stringSerde)
                )
                .selectKey { _, v -> v.serverIp }
                .to(CONNECTIONS_REKEY_TOPIC, Produced.with(stringSerde, connectionSerde))

        // Invalid connections (no ip nor name)
        connections[2]
                .peek { k, v -> logger.error("Received invalid connections: $k - $v") }

        return streamBuilder
                .stream<String, Connection>(CONNECTIONS_REKEY_TOPIC, Consumed.with(stringSerde, connectionSerde))
                .groupByKey()
                .aggregate(
                        { Server() },
                        { k, c, s ->
                            s.apply {
                                this.ip = k
                                this.sourceIps.add(ConnectionEvent(c.sourceIp))
                            }
                        },
                        Materialized.`as`<String, Server>(Stores.persistentKeyValueStore(SITES_AGGREGATIONS_TOPIC))
                                .withKeySerde(stringSerde)
                                .withValueSerde(serverSerde)
                )
                .toStream()
                .apply { this.to(SITES_TOPIC, Produced.with(stringSerde, serverSerde)) }
    }

    /**
     * Create a new json Serde for the given [clazz].
     */
    private fun <T> jsonSerde(clazz: Class<T>) =
            Serdes.serdeFrom(JsonSerializer(mapper), JsonDeserializer(clazz, mapper, false))
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
