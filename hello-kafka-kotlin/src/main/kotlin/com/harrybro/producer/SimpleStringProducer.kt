package com.harrybro.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.*
import java.util.concurrent.Future

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"

fun main() {
//    SimpleStringProducer(getKafkaStringSerializerConfig(BOOTSTRAP_SERVER))
//        .use { it.send(TOPIC_NAME, "hello kafka") }
//        .also { println("Message transmission was successful.") }
}

class SimpleStringProducer(private val config: Properties) : Closeable {
    private val producer = KafkaProducer<String, String>(this.config)

    fun send(topicName: String, message: String): Future<RecordMetadata> =
        this.producer.send(ProducerRecord(topicName, message))

    override fun close() = this.producer.close()
}

private fun getKafkaStringSerializerConfig(bootstrapServer: String) = Properties().apply {
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
}
