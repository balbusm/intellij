package io.confluent.intellijplugin.registry.custom

import com.intellij.openapi.Disposable
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

/**
 * Implemented by a class shipped in a user-provided JAR (loaded via [java.util.ServiceLoader])
 * to plug a third-party schema registry (e.g. Hortonworks/Cloudera Schema Registry) into
 * message consume/produce, without the plugin depending on that registry's client libraries.
 */
interface CustomRegistryClient : Disposable {
    fun configure(params: Map<String, String>)
    fun connect(calledByUser: Boolean)
    fun checkConnection()
    fun createDeserializer(format: KafkaRegistryFormat): Deserializer<Any>
    fun createSerializer(format: KafkaRegistryFormat): Serializer<Any>
}
