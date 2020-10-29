package com.plarium.south.superflow.core.spec.source.kafka.serde;

import org.apache.avro.Schema;

import java.util.Map;


public interface AvroConfigFetcher {

    String ALLOW_NO_STRICT = "allow.no.strict";
    String AVRO_SCHEMA_STRING = "avro.schema.string";
    String ALLOW_SCHEMA_EVO = "allow.schema.evolution";


    default boolean fetchSchemaEvo(Map<String, ?> configs) {
        if (configs.containsKey(ALLOW_SCHEMA_EVO)) {
            return (Boolean) configs.get(ALLOW_SCHEMA_EVO);
        }

        return false;
    }

    default Schema fetchAvroSchema(Map<String, ?> configs) {
        String schemaString = (String) configs.get(AVRO_SCHEMA_STRING);
        return new Schema.Parser().parse(schemaString);
    }

    default boolean fetchAllowNoStrict(Map<String, ?> configs) {
        if (configs.containsKey(ALLOW_NO_STRICT)) {
            return (Boolean) configs.get(ALLOW_NO_STRICT);
        }

        return false;
    }


    class AvroSchemaChangedException extends RuntimeException {
        private static final String ERROR_MSG =
                "Schema evolution not allowed, " +
                        "check allow.schema.evolution config, " +
                        "current schema: \n%s, new schema: \n%s";

        public AvroSchemaChangedException(Schema currentSchema, Schema newSchema) {
            super(String.format(ERROR_MSG, currentSchema.toString(), newSchema.toString()));
        }
    }
}
