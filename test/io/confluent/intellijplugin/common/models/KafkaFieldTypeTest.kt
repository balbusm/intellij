package io.confluent.intellijplugin.common.models

import com.intellij.testFramework.junit5.TestApplication
import io.confluent.intellijplugin.client.KafkaClient
import io.confluent.intellijplugin.consumer.models.ConsumerProducerFieldConfig
import io.confluent.intellijplugin.data.KafkaDataManager
import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.KafkaRegistryType
import io.confluent.intellijplugin.registry.custom.CustomRegistryClient
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.mockito.kotlin.*

@TestApplication
class KafkaFieldTypeTest {

    private fun fieldConfig(schemaFormat: KafkaRegistryFormat) = ConsumerProducerFieldConfig(
        type = KafkaFieldType.SCHEMA_REGISTRY,
        valueText = "",
        isKey = false,
        topic = "test-topic",
        registryType = KafkaRegistryType.CUSTOM,
        schemaName = "test-schema",
        schemaFormat = schemaFormat,
        parsedSchema = null
    )

    private fun mockDataManager(customRegistryClient: CustomRegistryClient?): KafkaDataManager {
        val mockClient = mock<KafkaClient> {
            on { this.customRegistryClient } doReturn customRegistryClient
        }
        return mock<KafkaDataManager> {
            on { client } doReturn mockClient
            on { registryType } doReturn KafkaRegistryType.CUSTOM
        }
    }

    @Nested
    @DisplayName("getDeserializationClass - CUSTOM registry")
    inner class Deserialization {

        @Test
        fun `should delegate to customRegistryClient createDeserializer with matching format`() {
            @Suppress("UNCHECKED_CAST")
            val expectedDeserializer = mock<Deserializer<Any>>()
            val customClient = mock<CustomRegistryClient> {
                on { createDeserializer(KafkaRegistryFormat.AVRO) } doReturn expectedDeserializer
            }
            val dataManager = mockDataManager(customClient)

            val result = KafkaFieldType.SCHEMA_REGISTRY.getDeserializationClass(
                dataManager, fieldConfig(KafkaRegistryFormat.AVRO)
            )

            assertSame(expectedDeserializer, result)
            verify(customClient).createDeserializer(KafkaRegistryFormat.AVRO)
        }

        @Test
        fun `should throw when customRegistryClient is not connected`() {
            val dataManager = mockDataManager(null)

            assertThrows(IllegalStateException::class.java) {
                KafkaFieldType.SCHEMA_REGISTRY.getDeserializationClass(
                    dataManager, fieldConfig(KafkaRegistryFormat.JSON)
                )
            }
        }
    }

    @Nested
    @DisplayName("getSerializer - CUSTOM registry")
    inner class Serialization {

        @Test
        fun `should delegate to customRegistryClient createSerializer with matching format`() {
            val expectedSerializer = mock<Serializer<Any>>()
            val customClient = mock<CustomRegistryClient> {
                on { createSerializer(KafkaRegistryFormat.PROTOBUF) } doReturn expectedSerializer
            }
            val dataManager = mockDataManager(customClient)

            val result = KafkaFieldType.SCHEMA_REGISTRY.getSerializer(
                dataManager, fieldConfig(KafkaRegistryFormat.PROTOBUF)
            )

            assertSame(expectedSerializer, result)
            verify(customClient).createSerializer(KafkaRegistryFormat.PROTOBUF)
        }

        @Test
        fun `should throw when customRegistryClient is not connected`() {
            val dataManager = mockDataManager(null)

            assertThrows(IllegalStateException::class.java) {
                KafkaFieldType.SCHEMA_REGISTRY.getSerializer(
                    dataManager, fieldConfig(KafkaRegistryFormat.JSON)
                )
            }
        }
    }
}
