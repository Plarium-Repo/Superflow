package com.plarium.south.superflow.core.utils;

import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

public class HortonRegistryUtils {

    private HortonRegistryUtils() {}

    public static SchemaRegistryClient createClient(String url) {
        Map<String, String> conf = new HashMap<>();
        conf.put(SCHEMA_REGISTRY_URL.name(), url);
        return new SchemaRegistryClient(conf);
    }

    public static boolean hasSchema(SchemaRegistryClient client, String schemaName) {
        try {
            client.getLatestSchemaVersionInfo(schemaName);
            return true;
        } catch (SchemaNotFoundException e) {
            return false;
        }
    }

    public static SchemaVersionInfo getLatestSchemaInfo(SchemaRegistryClient client, String schemaName) {
        try {
            return client.getLatestSchemaVersionInfo(schemaName);
        } catch (SchemaNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static SchemaVersionInfo getSchemaInfo(SchemaRegistryClient client, String schemaName, Integer version) {
        try {
            return client.getSchemaVersionInfo(new SchemaVersionKey(schemaName, version));
        } catch (SchemaNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Collection<SchemaVersionInfo> getAllSchemaInfo(SchemaRegistryClient client, String schemaName) {
        try {
            return client.getAllVersions(schemaName);
        } catch (SchemaNotFoundException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
