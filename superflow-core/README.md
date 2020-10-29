### Superflow - Apache Beam pipeline generator

Superflow is a simple tool that can generate and run Apache Beam pipelines without writing any code using the yaml definition.

#### Project Organization
    superflow-core
    ├── examples
    ├── src
    
#### Getting Started
Copy project on local machine via git clone, you need to have Java 1.8.

#### Building projects
```
tests: ./gradlew test
build: ./gradlew clean build
build docker: ./gradlew buildDocker
build all in one jar: ./gradlew ./gradlew shadowJar
run build without tests: ./gradlew clean build -x test
```

#### Usage
Superflow uses yaml as a declarative language for construct beam pipelines.

##### Flow Environment
Superflow support templating.
```yaml
---
type: flow/env
env:
  curr.date: ${curr.date}
  registry.url: http://localhost/api/v1
  input.location: gs://bucket/input/data
  output.location: gs://bucket/output/data  
```
 For use it in flow wrap value key in construction ${value_key} (${curr.date} for example above)
 > Notice: You must declare flow environment variables in separate file and pass it path via flow options
 
##### Flow Options
 Flow contains superflow and apache beam runners options
 
 ```yaml
 ---
 type: flow/options
 options:
   dryRun: true
   numWorkers: 5
   diskSizeGb: ${disk.size.gb}
   enforceImmutability: false
   enforceEncodability: false
   targetParallelism: ${num.local.core}
```
 
Superflow options:
- env (file path with yaml env)
- spec (file path with yaml spec)
- envMap (env in string map: {"dryRun":"false"})
- dryRun (Print output dataset without save result)

 Runner options can be mixed for different runners, for example targetParallelism option for DirectRunner, numWorkers option for DataflowRunner.
 Also options can be passed as command line arguments like as --runner=DirectRunner. It does configuration more elastic, because you don't have to change flow file.
 
 ##### Flow ETL
 This is the definition of Apache Beam flow, here we can see several main components:     
- **registry** - it give access for schemas (avro, json, etc),  
- **window** - logically separated elements on time windows
- **source** - read and convert input data in flow format
- **processor** - transform input data (for example sql query)
- **sink** - write data in store (files, store, database, etc)

```yaml
---
type: flow/etl
name: ${flow.name}
registry:
  type: registry/horton
  url: ${registry.url}
window:
  type: window/fixed
  size: 1 hour
  timeField: timestamp
source:
  type: source/fs/avro
  name: read-transactions
  location: ${source.location}
  mapping:
    - tag: transactions
      schema: ${input.schema.name}
      path: /${curr.date}/**
processor:
  type: processor/sql
  outputTag: sql_result
  name: transform-counts
  sqlExpression: |
    SELECT
      userId as user_id,
      sessionId as session_id,
      COUNT(trasactionId) as thx_count
    FROM transactions
    GROUP BY userId, sessionId
sink:
    type: sink/fs/avro
    name: write-counts
    tempDir: ${output.tmp.location}
    location:  ${output.location}
    numOfShards: ${sink.num.shards}
    mapping:
      - tag: transactions
        path: ${output.location}
```
> Notice: You can define a few flow/etl definition in one file  
See more examples in ./examples and ./src/main/resources/templates

 ##### Execute superflow jobs
For run job you must past 3 required parameters: runner, spec and (env or envMap).  

You can execute superflow flow some ways:
 
1. java -jar superflow-core-X.X.X-all.jar \  
   --env=path/to/env.yaml \  
   --spec=gs://bucket/spec.yaml \  
   --runner=DirectRunner 
   --numWorkers\  

2. docker run superfow-core:latest \  
   --runner=DirectRunner \  
   --env=gs://bucket/env.yaml \  
   --spec=/path/to/spec.yaml \  

3. curl -X POST \  
   "http://host:port/api/v1/submit?runner=DirectRunner" \  
   -H "Content-Type: multipart/form-data" \  
   -F "env=@/path/to/env.yaml;type=application/x-yaml" \  
   -F "spec=@/path/to/spec.yaml;type=application/x-yaml" \  

>Notice: You can replace existing flow options
>  
>For example we have numWorkers=5 param in flow/options section in spec.yaml file.  
>We can override it without modify flow file just past it value from command line:   
>docker run superflow-core:latest --numWorkers=3 --runner=DirectRunner --env=env.yaml, --spec=spec.yaml

#### Versioning
We use [SemVer](http://semver.org/) for versioning.

#### Authors
* **Maxim Tsygan**