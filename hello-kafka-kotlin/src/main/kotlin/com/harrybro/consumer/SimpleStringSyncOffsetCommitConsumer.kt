package com.harrybro.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    SimpleStringSyncOffsetCommitConsumer(getKafkaNonAutoCommitConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID)).also {
        it.subscribe(mutableListOf(TOPIC_NAME))
        var currentOffset: MutableMap<TopicPartition, OffsetAndMetadata>? = null
        var consumerRecords: ConsumerRecords<String, String>
        while (true) {
            consumerRecords = it.poll()
            if (consumerRecords.count() != 0) {
                for (consumerRecord in consumerRecords) {
                    println(consumerRecord)
                    currentOffset = hashMapOf(
                        TopicPartition(consumerRecord.topic(), consumerRecord.partition()).also(::println) to
                                OffsetAndMetadata(consumerRecord.offset() + 1).also(::println)
                    )
                }
                it.commitSync(currentOffset!!)
            }
        }
    }
}

class SimpleStringSyncOffsetCommitConsumer(config: Properties) : SimpleStringConsumer(config) {
    fun commitSync(offsets: MutableMap<TopicPartition, OffsetAndMetadata>) = super.consumer.commitSync(offsets)
}