package com.plarium.south.superflow.core.spec.source.mapping;

import com.plarium.south.superflow.core.spec.mapping.BaseMapping;

import javax.annotation.Nullable;

public class BaseSourceMapping extends BaseMapping {

    public BaseSourceMapping(String tag, @Nullable String eventTime) {
        super(tag, eventTime);
    }
}
