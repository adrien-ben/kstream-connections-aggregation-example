package com.adrienben.demo.kstreamconnections

import java.time.LocalDateTime

data class Connection(
        val sourceIp: String = "0.0.0.0",
        var serverIp: String? = null,
        var serverName: String? = null
)

data class Server(
        var ip: String = "0.0.0.0",
        val sourceIps: MutableList<ConnectionEvent> = mutableListOf()
)

data class ConnectionEvent(
        val ip: String = "0.0.0.0",
        val timestamp: LocalDateTime = LocalDateTime.now()
)
