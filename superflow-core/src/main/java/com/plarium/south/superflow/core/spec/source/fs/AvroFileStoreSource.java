package com.plarium.south.superflow.core.spec.source.fs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.source.BasePTupleRowSource;
import com.plarium.south.superflow.core.spec.dofn.AvroToBeamRowFn;
import com.plarium.south.superflow.core.spec.source.mapping.FsSourceMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SOURCE;

@SchemaDefinition(
        type = SOURCE,
        baseTypes = {"registry", "window"},
        required = {"type", "location", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "allowNoStrict", "location", "mapping"})
public class AvroFileStoreSource extends BasePTupleRowSource {
    public static final String TYPE = "source/fs/avro";

    @Getter @NotNull
    @JsonPropertyDescription("Data location: /path, gs://path")
    private final String location;

    @Getter @NotNull
    @JsonPropertyDescription("Allow process files with different schemas")
    private final Boolean allowNoStrict;

    @Getter @NotEmpty
    @JsonPropertyDescription("Directory to dataset mapping")
    private final MappingList<FsSourceMapping> mapping;


    @JsonCreator
    public AvroFileStoreSource(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "allowNoStrict") Boolean allowNoStrict,
            @JsonProperty(value = "location", required = true) String location,
            @JsonProperty(value = "mapping", required = true) MappingList<FsSourceMapping> mapping)
    {
        super(name, registry, window);
        this.location = location;
        this.mapping = mapping;
        this.allowNoStrict = firstNonNull(allowNoStrict, false);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void setup() {
        mapping.setup();
    }

    @Override
    public PCollectionTuple expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        PCollectionTuple datasets = PCollectionTuple.empty(pipeline);

        for (FsSourceMapping map : mapping.getList()) {
            Schema beamSchema = getBeamSchema(map);
            AvroIO.Read<GenericRecord> reader = createReader(map);
            PCollection<Row> dataset = applyOutputPart(pipeline, reader, beamSchema);

            dataset = applyWindow(dataset);
            dataset = applyTimePolicy(dataset, map.getEventTime());
            datasets = datasets.and(map.getTag(), dataset);
        }

        return datasets;
    }

    private Schema getBeamSchema(FsSourceMapping map) {
        String avroSchemaString = registry.getSchema(map.getSchema());

        org.apache.avro.Schema avroSchema =
                new org.apache.avro.Schema.Parser().parse(avroSchemaString);

        return AvroUtils.toBeamSchema(avroSchema);
    }

    private AvroIO.Read<GenericRecord> createReader(FsSourceMapping mapping) {
        String avroSchemaString = registry.getSchema(mapping.getSchema());

        return AvroIO.readGenericRecords(avroSchemaString)
                .from(buildPath(mapping.getPath()));
    }

    private PCollection<Row> applyOutputPart(Pipeline pipeline, AvroIO.Read<GenericRecord> reader, Schema beamSchema) {
        return pipeline.apply(reader)
                .apply(ParDo.of(new AvroToBeamRowFn(beamSchema, allowNoStrict)))
                .setRowSchema(beamSchema);
    }

    private String buildPath(String directory) {
        return location + directory;
    }
}
