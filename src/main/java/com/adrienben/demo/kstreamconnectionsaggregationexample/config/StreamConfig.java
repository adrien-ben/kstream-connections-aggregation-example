package com.adrienben.demo.kstreamconnectionsaggregationexample.config;

import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.Connection;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.ConnectionEvent;
import com.adrienben.demo.kstreamconnectionsaggregationexample.domain.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Slf4j
@Configuration
@EnableKafkaStreams
public class StreamConfig {

	public static final String DNS_TOPIC = "dns";
	public static final String CONNECTIONS_TOPIC = "connections";
	public static final String CONNECTIONS_BY_IP_REKEY_TOPIC = "connections_by_ip_rekey";
	public static final String CONNECTIONS_BY_NAME_REKEY_TOPIC = "connections_by_name_rekey";
	public static final String SITES_TOPIC = "sites";

	private static final String DNS_STORE = "dns_store";
	private static final String SITES_AGGREGATIONS_STORE = "site_aggregations";

	private ObjectMapper mapper;

	public StreamConfig(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	@Bean
	public KStream<String, Server> kStream(StreamsBuilder streamBuilder) {
		// First we define the various serialization/deserialization objects we will need
		var stringSerde = Serdes.String();
		var stringConsumed = Consumed.with(stringSerde, stringSerde);
		var serverSerde = jsonSerde(Server.class);
		var serverProduced = Produced.with(stringSerde, serverSerde);
		var connectionSerde = jsonSerde(Connection.class);
		var connectionConsumed = Consumed.with(stringSerde, connectionSerde);
		var connectionProduced = Produced.with(stringSerde, connectionSerde);

		// Here we consumed the topic containing the hostname to ip mapping as a KTable.
		var dnsTable = streamBuilder
				.table(DNS_TOPIC, stringConsumed, materializedAsPersistentStore(DNS_STORE, stringSerde, stringSerde));

		// Here we consumed the connection topic and split it in 3 streams:
		// - The first contains the connection by ip address
		// - The second contains the connection by hostname
		// - The last contains the invalid connections
		// The keys of these messages are ignored.
		@SuppressWarnings("unchecked")
		var connections = streamBuilder
				.stream(CONNECTIONS_TOPIC, connectionConsumed)
				.branch(
						(k, connection) -> connection.getServerIp() != null,
						(k, connection) -> connection.getServerIp() == null && connection.getServerName() != null,
						(k, connection) -> connection.getServerIp() == null && connection.getServerName() == null);

		// Connections by ip.
		// These are just re-keyed and sent to a topic that will contain
		// all connections with the server ip as their key.
		connections[0]
				.selectKey((k, connection) -> connection.getServerIp())
				.to(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionProduced);

		// Connection with no Ip but a name.
		// Server ip must be resolved by name. It's done by:
		// - Re-keying the message by server name
		// - Sending them through a topic to ensure the messages are properly partitioned by server name
		// - Then we join the re-keyed message to the dns KTable and inject the server name in the connection message
		// - Then message is re-keyed again (this time by server ip) and sent to the same topic as messages from the previous branch
		connections[1]
				.selectKey((k, connection) -> connection.getServerName())
				// The through operation is not mandatory here, just the fact that we combine selectKey and a join operation
				// would trigger a re-keying of the message. The downside of not being explicit here is that the
				// topic name would be generated and might change as topology evolves.
				.through(CONNECTIONS_BY_NAME_REKEY_TOPIC, connectionProduced)
				.leftJoin(
						dnsTable,
						(connection, mapping) -> {
							connection.setServerIp(mapping);
							return connection;
						},
						Joined.with(stringSerde, connectionSerde, stringSerde)
				)
				.selectKey((k, connection) -> connection.getServerIp())
				.to(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionProduced);

		// Invalid connections (no ip nor name).
		// We just log and error for each of them.
		connections[2]
				.peek((k, connection) -> log.error("Received invalid connections: {}", connection));

		// Now we consume the connections by server ip topic and aggregate the connections events by server:
		// - First we group all messages by key.
		// - Then we aggregate them by setting the correct ip and adding a ConnectionEvent in the aggregated message.
		// - Finally we produce these aggregated in a final topic.
		var serverStream = streamBuilder
				.stream(CONNECTIONS_BY_IP_REKEY_TOPIC, connectionConsumed)
				.groupByKey()
				.aggregate(
						Server::new,
						(key, connection, server) -> {
							server.setIp(key);
							server.getSourceIps().add(new ConnectionEvent(connection.getSourceIp()));
							return server;
						},
						materializedAsPersistentStore(SITES_AGGREGATIONS_STORE, stringSerde, serverSerde)
				)
				.toStream();

		serverStream.to(SITES_TOPIC, serverProduced);

		return serverStream;
	}

	private <T> Serde<T> jsonSerde(Class<T> targetClass) {
		return Serdes.serdeFrom(
				new JsonSerializer<>(mapper),
				new JsonDeserializer<>(targetClass, mapper, false)
		);
	}

	private <K, V> Materialized<K, V, KeyValueStore<Bytes, byte[]>> materializedAsPersistentStore(
			String storeName,
			Serde<K> keySerde,
			Serde<V> valueSerde
	) {
		return Materialized.<K, V>as(Stores.persistentKeyValueStore(storeName))
				.withKeySerde(keySerde)
				.withValueSerde(valueSerde);
	}
}
