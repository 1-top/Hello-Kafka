package com.harrybro.admin

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.common.config.ConfigResource
import java.util.*

private const val BOOTSTRAP_SERVER = "my-kafka:9092"

// get broker information
fun main() {
    val adminClient = AdminClient.create(Properties().apply { put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER) })
    adminClient.describeCluster().nodes().get().forEach {
        val configResource = ConfigResource(ConfigResource.Type.BROKER, it.idString())
        val describeConfigs = adminClient.describeConfigs(Collections.singleton(configResource))
        describeConfigs.all().get().forEach { (broker, config) ->
            config.entries().forEach {configEntry -> println("${configEntry.name()} : ${configEntry.value()}") }
        }
    }
}