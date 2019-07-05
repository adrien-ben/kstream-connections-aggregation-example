package com.adrienben.demo.kstreamconnections

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.kafka.test.EmbeddedKafkaBroker
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.kafka.test.utils.KafkaTestUtils.*
import java.time.Duration

@SpringBootTest
@EmbeddedKafka(
        topics = [DNS_TOPIC, CONNECTIONS_TOPIC, CONNECTIONS_BY_IP_REKEY_TOPIC, CONNECTIONS_BY_NAME_REKEY_TOPIC, SITES_TOPIC],
        ports = [9092]
)
class AppTests @Autowired constructor(
        private val embeddedKafka: EmbeddedKafkaBroker,
        private val mapper: ObjectMapper
) {
    @Test
    fun integrationTest() {
        val connectionProducer = createProducer(ByteArraySerializer(), JsonSerializer<Connection>(mapper))
        val dnsProducer = createProducer(StringSerializer(), StringSerializer())
        val siteConsumer = createConsumer(SITES_TOPIC, StringDeserializer(), JsonDeserializer(Server::class.java, mapper, false))

        connectionProducer.send(ProducerRecord(CONNECTIONS_TOPIC, Connection("123.456.78.90"))).get()
        connectionProducer.send(ProducerRecord(CONNECTIONS_TOPIC, Connection("123.456.78.90", serverName = "localhost"))).get()
        connectionProducer.send(ProducerRecord(CONNECTIONS_TOPIC, Connection("123.456.78.91", serverIp = "127.0.0.1"))).get()

        val preDnsServer = getSingleRecord(siteConsumer, SITES_TOPIC, Duration.ofSeconds(30).toMillis())

        dnsProducer.send(ProducerRecord(DNS_TOPIC, "localhost", "127.0.0.1")).get()
        connectionProducer.send(ProducerRecord(CONNECTIONS_TOPIC, Connection("123.456.78.92", serverName = "localhost"))).get()

        val postDnsServer = getSingleRecord(siteConsumer, SITES_TOPIC, Duration.ofSeconds(30).toMillis())

        assertAll(
                // Assert message correctness before dns topic was supplied any message
                { assertEquals("127.0.0.1", preDnsServer.key()) },
                { assertEquals("127.0.0.1", preDnsServer.value().ip) },
                { assertEquals(1, preDnsServer.value().sourceIps.size) },
                { assertEquals("123.456.78.91", preDnsServer.value().sourceIps[0].ip) }
        )

        assertAll(
                // Assert message correctness after dns topic was supplied any message
                { assertEquals("127.0.0.1", postDnsServer.key()) },
                { assertEquals("127.0.0.1", postDnsServer.value().ip) },
                { assertEquals(2, postDnsServer.value().sourceIps.size) },
                { assertEquals("123.456.78.91", postDnsServer.value().sourceIps[0].ip) },
                { assertEquals("123.456.78.92", postDnsServer.value().sourceIps[1].ip) }
        )
    }

    private fun <K, V> createProducer(
            keySerializer: Serializer<K>,
            valueSerializer: Serializer<V>
    ): Producer<K, V> {
        val producerProps = producerProps(embeddedKafka)
        val defaultKafkaProducerFactory = DefaultKafkaProducerFactory(producerProps, keySerializer, valueSerializer)
        return defaultKafkaProducerFactory.createProducer()
    }

    private fun <K, V> createConsumer(
            topic: String,
            keyDeserializer: Deserializer<K>,
            valueDeserializer: Deserializer<V>
    ): Consumer<K, V> {
        val consumerProps = consumerProps("integration-test", "true", embeddedKafka)
        val defaultKafkaConsumerFactory = DefaultKafkaConsumerFactory(consumerProps, keyDeserializer, valueDeserializer)
        return defaultKafkaConsumerFactory.createConsumer().apply {
            this.subscribe(mutableSetOf(topic))
        }
    }
}
