package io.confluent.intellijplugin.registry.custom

import io.confluent.intellijplugin.registry.KafkaRegistryFormat
import io.confluent.intellijplugin.registry.SchemaVersionInfo
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.registry.confluent.controller.TopicSchemaViewType
import io.confluent.kafka.schemaregistry.ParsedSchema
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class BrowsableCustomRegistryClientTest {

    private class MinimalBrowsableClient : BrowsableCustomRegistryClient {
        override fun configure(params: Map<String, String>) {}
        override fun connect(calledByUser: Boolean) {}
        override fun checkConnection() {}
        override fun createDeserializer(format: KafkaRegistryFormat): Deserializer<Any> = error("not used")
        override fun createSerializer(format: KafkaRegistryFormat): Serializer<Any> = error("not used")
        override fun resolveSchemaName(topic: String, viewType: TopicSchemaViewType) = topic
        override fun listSchemas(limit: Int?, filter: String?, connectionId: String) = emptyList<KafkaSchemaInfo>() to false
        override fun listSchemaVersions(schemaName: String) = emptyList<Long>()
        override fun getSchemaVersionInfo(schemaName: String, version: Long): SchemaVersionInfo = error("not used")
        override fun getLatestVersionInfo(schemaName: String): SchemaVersionInfo = error("not used")
        override fun loadSchemaInfo(schemaName: String): KafkaSchemaInfo = error("not used")
        override fun createSchema(schemaName: String, parsedSchema: ParsedSchema) {}
        override fun updateSchema(versionInfo: SchemaVersionInfo, newSchema: String) {}
        override fun deleteSchema(schemaName: String, permanent: Boolean) {}
        override fun deleteSchemaVersion(versionInfo: SchemaVersionInfo, isPermanent: Boolean) {}
        override fun dispose() {}
    }

    @Test
    fun `supportsKeySchema defaults to true when not overridden`() {
        assertTrue(MinimalBrowsableClient().supportsKeySchema())
    }
}
