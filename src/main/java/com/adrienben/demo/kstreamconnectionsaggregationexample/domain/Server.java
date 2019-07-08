package com.adrienben.demo.kstreamconnectionsaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Server {
	private String ip;
	private final List<ConnectionEvent> sourceIps = new ArrayList<>();
}
