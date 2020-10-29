package com.plarium.south.superflow.core.spec.commons.schema;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface SchemaDefinition {

    SpecType type();

    String[] required();

    String[] tags() default {};

    String description() default "";

    String[] baseTypes() default {};

    String[] ignoreFields() default {};
}
