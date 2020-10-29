package com.plarium.south.superflow.core.spec.flow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.collect.Maps;
import com.plarium.south.superflow.core.spec.commons.schema.SchemaDefinition;
import lombok.Getter;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.Map;

import static com.plarium.south.superflow.core.spec.commons.schema.SpecType.FLOW;

@SchemaDefinition(
        type = FLOW,
        required = {"type", "options"})

@JsonPropertyOrder({"type", "name", "options"})
public class FlowOptions extends RootElement {
    public static final String TYPE = "flow/options";

    private static final String ARGS_PATTERN = "--%s=%s";

    @Getter @NotEmpty
    private final Map<String, String> options;

    public FlowOptions(
            @JsonProperty(value = "name") String name,
            @JsonProperty(value = "options", required = true) Map<String, String> options)
    {
        super(name);
        this.options = options;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public String[] toArgs() {
        return options.entrySet()
                .stream()
                .map(this::toArg)
                .toArray(String[]::new);
    }

    public FlowOptions addArgsWithReplace(String[] args) {
        Map<String, String> newOptions = Maps.newHashMap();
        for (String arg : args) {
            arg = arg.replace("--", "");
            String[] split = arg.split("=");
            newOptions.put(split[0], split[1]);
        }

        options.putAll(newOptions);
        return this;
    }

    private String toArg(Map.Entry<String, String> arg) {
        return String.format(ARGS_PATTERN, arg.getKey(), arg.getValue());
    }
}
