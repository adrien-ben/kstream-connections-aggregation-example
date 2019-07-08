package com.adrienben.demo.kstreamconnectionsaggregationexample.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionEvent {
	private String ip;
	private final LocalDateTime timestamp = LocalDateTime.now();
}
