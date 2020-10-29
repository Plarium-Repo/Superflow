package com.plarium.south.superflow.core.spec.sink.fs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.mapping.MappingList;
import com.plarium.south.superflow.core.spec.registry.BaseRegistry;
import com.plarium.south.superflow.core.spec.sink.BasePTupleRowSink;
import com.plarium.south.superflow.core.spec.sink.mapping.FsSinkMapping;
import com.plarium.south.superflow.core.spec.window.BaseWindow;
import com.plarium.south.superflow.core.spec.dofn.BeamRowToAvroFn;
import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.SINK;
import static org.apache.beam.sdk.io.FileBasedSink.convertToFileResourceIfPossible;


@SchemaDefinition(
        type = SINK,
        ignoreFields = {"window", "registry"},
        required = {"type", "tempDir", "location", "mapping"})

@JsonPropertyOrder({"type", "name", "registry", "window", "tempDir", "location", "format", "numOfShards", "mapping"})
public class AvroFileStoreSink extends BasePTupleRowSink {
    public static final String TYPE = "sink/fs/avro";

    private static final String FILE_EXT = "avro";

    @Getter @NotNull
    @JsonPropertyDescription("Set the base directory used to generate temporary files.")
    private final String tempDir;

    @Getter @NotNull
    @JsonPropertyDescription("The location for output data")
    private final String location;

    @Getter @NotNull
    @JsonPropertyDescription("The file format based on java date templates")
    private final String format;

    @Getter
    @JsonPropertyDescription("The number of output files")
    private final Integer numOfShards;

    @Getter @NotNull
    @JsonPropertyDescription("Output mapping dataset to directory")
    private final MappingList<FsSinkMapping> mapping;


    @JsonCreator
    public AvroFileStoreSink(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "format") String format,
            @JsonProperty(value = "registry") BaseRegistry registry,
            @JsonProperty(value = "window") BaseWindow window,
            @JsonProperty(value = "numOfShards") Integer numOfShards,
            @JsonProperty(value = "createSchema") Boolean createSchema,
            @JsonProperty(value = "tempDir", required = true) String tempDir,
            @JsonProperty(value = "location", required = true) String location,
            @JsonProperty(value = "mapping", required = true) MappingList<FsSinkMapping> mapping)
    {
        super(name, registry, createSchema, window);
        this.format = format;
        this.tempDir = tempDir;
        this.location = location;
        this.mapping = mapping;
        this.numOfShards = firstNonNull(numOfShards, 1);
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
    public PDone expand(PCollectionTuple input) {
        for (FsSinkMapping map : mapping.getList()) {
            PCollection<Row> dataset = input.get(map.getTag());

            String schemaText = inferOrFetchSchema(map, dataset);
            Schema avroSchema = new Schema.Parser().parse(schemaText);
            org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(avroSchema);
            AvroIO.Write<GenericRecord> writer = createWriter(map.getPath(), avroSchema);

            applyWindow(applyTimePolicy(dataset, map.getEventTime()))
                .apply(ParDo.of(new BeamRowToAvroFn(beamSchema)))
                .setCoder(AvroCoder.of(avroSchema))
                .apply(writer);

            createSchemaIfNeeded(map.getSchema(), avroSchema.toString());
        }

        return PDone.in(input.getPipeline());
    }

    private AvroIO.Write<GenericRecord> createWriter(String directory, Schema avroSchema) {
        AvroIO.Write<GenericRecord> writer = AvroIO.writeGenericRecords(avroSchema)
                .to(new DefaultFileNamePolicy(format, buildPath(directory), FILE_EXT))
                .withTempDirectory(convertToFileResourceIfPossible(tempDir));

        if (numOfShards != null) {
            writer = writer.withNumShards(numOfShards);
        }

        if(windowIsNeed()) return writer;
        else return writer.withWindowedWrites();
    }

    private String buildPath(String directory) {
        return location + directory;
    }
}
