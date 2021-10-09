package com.harrybro.consumer

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.util.*

private const val TOPIC_NAME = "test"
private const val BOOTSTRAP_SERVER = "my-kafka:9092"
private const val GROUP_ID = "test-group"

fun main() {
    SimpleStringRebalanceSyncOffsetCommitConsumer(
        getKafkaNonAutoCommitConsumerConfig(BOOTSTRAP_SERVER, GROUP_ID)
    ).also {
        var currentOffset: MutableMap<TopicPartition, OffsetAndMetadata>? = null
        var consumerRecords: ConsumerRecords<String, String>
        it.subscribe(mutableListOf(TOPIC_NAME), object : ConsumerRebalanceListener {
            override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) =
                println("리밸런스 시작되기 직전에 실행되는 메서드")

            override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) =
                println("리밸런스가 끝난 뒤 파티션이 할당 완료되면 호출되는 메서드다.")
        })
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

class SimpleStringRebalanceSyncOffsetCommitConsumer(config: Properties) : SimpleStringConsumer(config) {
    fun subscribe(topicName: Collection<String>, listener: ConsumerRebalanceListener) =
        super.consumer.subscribe(topicName, listener)

    fun commitSync(offsets: MutableMap<TopicPartition, OffsetAndMetadata>) = super.consumer.commitSync(offsets)
}