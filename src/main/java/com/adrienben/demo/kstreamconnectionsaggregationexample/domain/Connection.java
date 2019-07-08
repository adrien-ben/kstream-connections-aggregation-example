package com.adrienben.demo.kstreamconnectionsaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Connection {

	private String sourceIp;
	private String serverIp;
	private String serverName;

	public static Connection byName(String sourceIp, String serverName) {
		return new Connection(sourceIp, null, serverName);
	}

	public static Connection byIp(String sourceIp, String serverIp) {
		return new Connection(sourceIp, serverIp, null);
	}
}
