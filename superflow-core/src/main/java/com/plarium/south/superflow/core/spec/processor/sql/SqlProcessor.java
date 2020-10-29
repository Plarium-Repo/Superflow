package com.plarium.south.superflow.core.spec.processor.sql;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import com.plarium.south.superflow.core.spec.processor.BasePTupleRowProcessor;
import com.plarium.south.superflow.core.spec.processor.sql.udaf.AnyLongAggUdaf;
import com.plarium.south.superflow.core.spec.processor.sql.udaf.UniqueCountUdaf;
import com.plarium.south.superflow.core.spec.processor.sql.udf.FormatTimestampUdf;
import com.plarium.south.superflow.core.spec.processor.sql.udf.StringDateToLongUdf;
import lombok.Getter;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.values.PCollectionTuple;

import javax.validation.constraints.NotNull;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.PROCESSOR;

@SchemaDefinition(
        type = PROCESSOR,
        ignoreFields = {"window", "registry"},
        required = {"type", "outputTag", "sqlExpression"})

@JsonPropertyOrder({"type", "name", "outputTag", "sqlExpression"})
public class SqlProcessor extends BasePTupleRowProcessor {
    public static final String TYPE = "processor/sql";

    @JsonPropertyDescription("The sql engine type")
    private final Boolean zetaSql;

    @Getter @NotNull
    @JsonPropertyDescription("The name of enriched dataset in output tuple")
    private final String outputTag;

    @Getter @NotNull
    @JsonPropertyDescription("Sql expression")
    private final String sqlExpression;


    @JsonCreator
    public SqlProcessor(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "zetaSql") Boolean zetaSql,
            @JsonProperty(value = "outputTag", required = true) String outputTag,
            @JsonProperty(value = "sqlExpression", required = true) String sqlExpression)
    {
        super(name, null, null);
        this.zetaSql = firstNonNull(zetaSql, false);
        this.outputTag = outputTag;
        this.sqlExpression = sqlExpression;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public PCollectionTuple expand(PCollectionTuple input) {
        enableZetaSqlIfNeeded(input);

        SqlTransform sqlTransform = SqlTransform.query(sqlExpression)
                .registerUdaf(AnyLongAggUdaf.NAME, new AnyLongAggUdaf())
                .registerUdaf(UniqueCountUdaf.NAME, new UniqueCountUdaf())
                .registerUdf(FormatTimestampUdf.NAME, FormatTimestampUdf.class)
                .registerUdf(StringDateToLongUdf.NAME, StringDateToLongUdf.class);

        return input.and(outputTag, input.apply(sqlTransform));
    }

    private void enableZetaSqlIfNeeded(PCollectionTuple input) {
        if (zetaSql) {
            input.getPipeline().getOptions().as(BeamSqlPipelineOptions.class)
                    .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
        }
    }
}
