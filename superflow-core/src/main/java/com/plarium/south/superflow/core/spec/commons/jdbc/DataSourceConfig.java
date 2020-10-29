package com.plarium.south.superflow.core.spec.commons.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Getter;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.validation.constraints.NotNull;
import java.io.Serializable;

import static com.plarium.south.superflow.core.utils.ClassLoaderUtils.getNewClassLoader;

@JsonPropertyOrder({"url", "driverClass", "username", "password","urlProperties", "driverJars"})
public class DataSourceConfig implements Serializable {

    @Getter @NotNull
    @JsonPropertyDescription("The url for connect to database")
    private final String url;

    @Getter @NotNull
    @JsonPropertyDescription("The data source driver class full name")
    private final String driverClass;

    @Getter
    @JsonPropertyDescription("The username for connect to database")
    private final String username;

    @Getter
    @JsonPropertyDescription("The password for connect to database")
    private final String password;

    @Getter
    @JsonPropertyDescription("Sets the connection properties. Format of the string must be key1=value1;...keyn=valuen;")
    private final String urlProperties;

    @Getter @NotNull
    @JsonPropertyDescription("The paths with needs jars for jdbc driver (path1,..,,pathn)")
    private final String driverJars;

    @JsonCreator
    public DataSourceConfig(
            @JsonProperty(value = "url", required = true) String url,
            @JsonProperty(value = "username", required = true) String username,
            @JsonProperty(value = "password", required = true) String password,
            @JsonProperty(value = "driverJars", required = true) String driverJars,
            @JsonProperty(value = "driverClass", required = true) String driverClass,
            @JsonProperty(value = "urlProperties") String urlProperties)
    {
        this.url = url;
        this.username = username;
        this.password = password;
        this.driverJars = driverJars;
        this.driverClass = driverClass;
        this.urlProperties = urlProperties;
    }

    public BasicDataSource buildDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setDriverClassName(driverClass);
        dataSource.setDriverClassLoader(getNewClassLoader(driverJars));

        if (urlProperties != null) {
            dataSource.setConnectionProperties(urlProperties);
        }

        return dataSource;
    }
}
