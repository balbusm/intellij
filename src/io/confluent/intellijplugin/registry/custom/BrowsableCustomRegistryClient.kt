package io.confluent.intellijplugin.registry.custom

import io.confluent.intellijplugin.registry.SchemaVersionInfo
import io.confluent.intellijplugin.registry.common.KafkaSchemaInfo
import io.confluent.intellijplugin.registry.confluent.controller.TopicSchemaViewType
import io.confluent.kafka.schemaregistry.ParsedSchema

/**
 * Optional extension of [CustomRegistryClient] for JAR implementations that also want to
 * support the Schema Registry browser (list/create/update/delete schemas). Not required for
 * basic consume/produce support.
 */
interface BrowsableCustomRegistryClient : CustomRegistryClient {
    /** True if key and value have separate schemas (two tabs on Topic Details); false for a single schema per topic. */
    fun supportsKeySchema(): Boolean = true

    /** Maps a topic name to the subject/schema name this registry uses for it. Naming convention is JAR-specific. */
    fun resolveSchemaName(topic: String, viewType: TopicSchemaViewType): String

    fun listSchemas(limit: Int?, filter: String?, connectionId: String): Pair<List<KafkaSchemaInfo>, Boolean>
    fun listSchemaVersions(schemaName: String): List<Long>
    fun getSchemaVersionInfo(schemaName: String, version: Long): SchemaVersionInfo
    fun getLatestVersionInfo(schemaName: String): SchemaVersionInfo
    fun loadSchemaInfo(schemaName: String): KafkaSchemaInfo
    fun createSchema(schemaName: String, parsedSchema: ParsedSchema)
    fun updateSchema(versionInfo: SchemaVersionInfo, newSchema: String)
    fun deleteSchema(schemaName: String, permanent: Boolean)
    fun deleteSchemaVersion(versionInfo: SchemaVersionInfo, isPermanent: Boolean = false)
}
