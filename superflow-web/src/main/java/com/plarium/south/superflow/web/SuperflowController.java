package com.plarium.south.superflow.web;

import com.plarium.south.superflow.core.Boot;
import com.plarium.south.superflow.core.spec.commons.schema.SpecSchemaInfo;
import com.plarium.south.superflow.core.spec.commons.schema.SpecType;
import com.plarium.south.superflow.core.utils.SpecSchemaUtils;
import io.micrometer.core.annotation.Timed;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.plarium.south.superflow.core.utils.SpecSchemaUtils.getSchemaInfo;

@Slf4j
@RestController
@RequestMapping("/api/v1")
public class SuperflowController {

    @Timed
    @GetMapping(value = "/schema")
    public ResponseEntity<String> getSchema(
            @RequestParam("type") String type,
            @RequestParam(name = "dropBase", required = false, defaultValue = "false") Boolean dropBase) {
        SpecSchemaInfo info = getSchemaInfo(type);
        String yamlSchema = info.yamlSchema(dropBase);
        return ResponseEntity.ok(yamlSchema);
    }

    @Timed
    @GetMapping(value = "/schema/info")
    public ResponseEntity<SpecSchemaInfo> getInfo(@RequestParam("type") String type) {
        return ResponseEntity.ok(getSchemaInfo(type));
    }

    @Timed
    @GetMapping(value = "/subtypes/{type}")
    public ResponseEntity<List<String>> getSubtypes(@PathVariable("type") String type) {
        return ResponseEntity.ok(SpecSchemaUtils.getSubtypes(SpecType.valueOf(type.toUpperCase())));
    }

    @Timed
    @PostMapping(value = "/submit")
    public void submit(
                       @RequestParam("env") MultipartFile env,
                       @RequestParam("spec") MultipartFile spec,
                       @RequestParam Map<String,String> options) throws IOException
    {
        File envFile = null;
        File specFile = null;

        try {
            envFile = transferFile(env);
            specFile = transferFile(spec);

            options.put("env", envFile.getAbsolutePath());
            options.put("spec", specFile.getAbsolutePath());
            String[] bootArgs = optionsToString(options);
            Boot.main(bootArgs);
        }
        finally {
            deleteFile(envFile);
            deleteFile(specFile);
        }
    }

    private File transferFile(MultipartFile mf) throws IOException {
        File file = File.createTempFile("env", "superflow");
        mf.transferTo(file);
        file.deleteOnExit();
        return file;
    }

    private void deleteFile(File file) {
        if (file == null) return;
        if (!file.delete()) log.warn("Unable to delete file: {}", file);
    }

    private String[] optionsToString(Map<String, String> options) {
        return options.entrySet()
                .stream()
                .map(this::toArg)
                .toArray(String[]::new);
    }

    private static final String ARGS_PATTERN = "--%s=%s";

    private String toArg(Map.Entry<String, String> arg) {
        return String.format(ARGS_PATTERN, arg.getKey(), arg.getValue());
    }
}
