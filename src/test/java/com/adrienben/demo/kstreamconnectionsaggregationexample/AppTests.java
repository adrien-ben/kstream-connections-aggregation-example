package com.adrienben.demo.kstreamconnectionsaggregationexample;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.Connection;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.CONNECTIONS_BY_IP_REKEY_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.CONNECTIONS_BY_NAME_REKEY_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.CONNECTIONS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.DNS_TOPIC;
import static com.adrienben.demo.kstreamconnectionsaggregationexample.config.StreamConfig.SITES_TOPIC;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(
		topics = { DNS_TOPIC, CONNECTIONS_TOPIC, CONNECTIONS_BY_IP_REKEY_TOPIC, CONNECTIONS_BY_NAME_REKEY_TOPIC, SITES_TOPIC },
		ports = { 9092 }
)
public class AppTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private ObjectMapper mapper;

	@Test
	void integrationTest() throws ExecutionException, InterruptedException {
		var connectionProducer = createProducer(new ByteArraySerializer(), new JsonSerializer<Connection>(mapper));
		var dnsProducer = createProducer(new StringSerializer(), new StringSerializer());
		var siteConsumer = createConsumer(SITES_TOPIC, new StringDeserializer(), new JsonDeserializer<>(Server.class, mapper, false));

		connectionProducer.send(new ProducerRecord<>(CONNECTIONS_TOPIC, Connection.byIp("123.456.78.90", null))).get();
		connectionProducer.send(new ProducerRecord<>(CONNECTIONS_TOPIC, Connection.byName("123.456.78.90", "localhost"))).get();
		connectionProducer.send(new ProducerRecord<>(CONNECTIONS_TOPIC, Connection.byIp("123.456.78.91", "127.0.0.1"))).get();

		var preDnsServer = KafkaTestUtils.getSingleRecord(siteConsumer, SITES_TOPIC, Duration.ofSeconds(30).toMillis());

		dnsProducer.send(new ProducerRecord<>(DNS_TOPIC, "localhost", "127.0.0.1")).get();
		connectionProducer.send(new ProducerRecord<>(CONNECTIONS_TOPIC, Connection.byName("123.456.78.92", "localhost"))).get();

		var postDnsServer = KafkaTestUtils.getSingleRecord(siteConsumer, SITES_TOPIC, Duration.ofSeconds(30).toMillis());

		assertAll(
				// Assert message correctness before dns topic was supplied any message
				() -> assertEquals("127.0.0.1", preDnsServer.key()),
				() -> assertEquals("127.0.0.1", preDnsServer.value().getIp()),
				() -> assertEquals(1, preDnsServer.value().getSourceIps().size()),
				() -> assertEquals("123.456.78.91", preDnsServer.value().getSourceIps().get(0).getIp())
		);

		assertAll(
				// Assert message correctness after dns topic was supplied any message
				() -> assertEquals("127.0.0.1", postDnsServer.key()),
				() -> assertEquals("127.0.0.1", postDnsServer.value().getIp()),
				() -> assertEquals(2, postDnsServer.value().getSourceIps().size()),
				() -> assertEquals("123.456.78.91", postDnsServer.value().getSourceIps().get(0).getIp()),
				() -> assertEquals("123.456.78.92", postDnsServer.value().getSourceIps().get(1).getIp())
		);
	}

	private <K, V> Producer<K, V> createProducer(
			Serializer<K> keySerializer,
			Serializer<V> valueSerializer
	) {
		var producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		var defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(producerProps, keySerializer, valueSerializer);
		return defaultKafkaProducerFactory.createProducer();
	}

	private <K, V> Consumer<K, V> createConsumer(
			String topic,
			Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer
	) {
		var consumerProps = KafkaTestUtils.consumerProps("integration-test", "true", embeddedKafka);
		var defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, keyDeserializer, valueDeserializer);
		var consumer = defaultKafkaConsumerFactory.createConsumer();
		consumer.subscribe(Set.of(topic));
		return consumer;
	}
}
