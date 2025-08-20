<p align="center">
  <a href="https://nightlies.apache.org/flink/flink-cdc-docs-stable/"><img src="docs/static/fig/flinkcdc-logo.png" alt="Flink CDC" style="width: 375px;"></a>
</p>
<p align="center">
<a href="https://github.com/apache/flink-cdc/" target="_blank">
    <img src="https://img.shields.io/github/stars/apache/flink-cdc?style=social&label=Star&maxAge=2592000" alt="Test">
</a>
<a href="https://github.com/apache/flink-cdc/releases" target="_blank">
    <img src="https://img.shields.io/github/v/release/apache/flink-cdc?color=yellow" alt="Release">
</a>
<a href="https://github.com/apache/flink-cdc/actions/workflows/flink_cdc_ci.yml" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/apache/flink-cdc/flink_cdc_ci.yml?branch=master" alt="Build">
</a>
<a href="https://github.com/apache/flink-cdc/actions/workflows/flink_cdc_ci_nightly.yml" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/apache/flink-cdc/flink_cdc_ci_nightly.yml?branch=master&label=nightly" alt="Nightly Build">
</a>
<a href="https://github.com/apache/flink-cdc/tree/master/LICENSE" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=Apache License 2.0&color=white" alt="License">
</a>
</p>


Flink CDC is a distributed data integration tool for real time data and batch data. Flink CDC brings the simplicity 
and elegance of data integration via YAML to describe the data movement and transformation in a 
[Data Pipeline](docs/content/docs/core-concept/data-pipeline.md).


The Flink CDC prioritizes efficient end-to-end data integration and offers enhanced functionalities such as 
full database synchronization, sharding table synchronization, schema evolution and data transformation.

![Flink CDC framework design](docs/static/fig/architecture.png)

### Getting Started

1. Prepare a [Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/docs/try-flink/local_installation/#starting-and-stopping-a-local-cluster) cluster and set up `FLINK_HOME` environment variable.
2. [Download](https://github.com/apache/flink-cdc/releases) Flink CDC tar, unzip it and put jars of pipeline connector to Flink `lib` directory.

> If you're using macOS or Linux, you may use `brew install apache-flink-cdc` to install Flink CDC and compatible connectors quickly.

3. Create a **YAML** file to describe the data source and data sink. Here are examples for synchronizing databases to Amazon DSQL:

#### MySQL to DSQL Example
  ```yaml
   source:
     type: mysql
     hostname: <mysql-host>
     port: 3306
     username: <username>
     password: <password>
     tables: <database>.<table>
     server-id: 1
     heartbeat.interval: 30s

   sink:
     type: dsql
     host: <dsql-cluster-endpoint>.dsql.<region>.on.aws
     port: 5432
     database: postgres
     schema: public
     use-iam-auth: true
     region: <aws-region>

   pipeline:
     name: Sync MySQL to Amazon DSQL
     parallelism: 1
  ```

#### PostgreSQL to DSQL Example
  ```yaml
   source:
     type: postgres
     hostname: <postgres-host>
     port: 5432
     username: <username>
     password: <password>
     database: <database>
     schema: public
     tables: <table1>,<table2>
     slot.name: "flink_cdc_slot"
     decoding.plugin.name: "pgoutput"

   sink:
     type: dsql
     host: <dsql-cluster-endpoint>.dsql.<region>.on.aws
     port: 5432
     database: postgres
     schema: public
     use-iam-auth: true
     region: <aws-region>

   pipeline:
     name: Sync PostgreSQL to Amazon DSQL
     parallelism: 1
  ```
4. Submit pipeline job using `flink-cdc.sh` script.
 ```shell
  # For MySQL to DSQL
  bash cd $FLINK_BASE && ./run-cdc.sh mysql-to-dsql.yaml
  
  # For PostgreSQL to DSQL
  bash cd $FLINK_BASE && ./run-cdc.sh postgresql-to-dsql.yaml
 ```
5. View job execution status through Flink WebUI or downstream database.

Try it out yourself with our more detailed [tutorial](docs/content/docs/get-started/quickstart/mysql-to-doris.md). 
You can also see [connector overview](docs/content/docs/connectors/pipeline-connectors/overview.md) to view a comprehensive catalog of the
connectors currently provided and understand more detailed configurations.

### Join the Community

There are many ways to participate in the Apache Flink CDC community. The
[mailing lists](https://flink.apache.org/what-is-flink/community/#mailing-lists) are the primary place where all Flink
committers are present. For user support and questions use the user mailing list. If you've found a problem of Flink CDC,
please create a [Flink jira](https://issues.apache.org/jira/projects/FLINK/summary) and tag it with the `Flink CDC` tag.   
Bugs and feature requests can either be discussed on the dev mailing list or on Jira.



### Contributing

Welcome to contribute to Flink CDC, please see our [Developer Guide](docs/content/docs/developer-guide/contribute-to-flink-cdc.md)
and [APIs Guide](docs/content/docs/developer-guide/understand-flink-cdc-api.md).



### License

[Apache 2.0 License](LICENSE).



### Special Thanks

The Flink CDC community welcomes everyone who is willing to contribute, whether it's through submitting bug reports,
enhancing the documentation, or submitting code contributions for bug fixes, test additions, or new feature development.     
Thanks to all contributors for their enthusiastic contributions.

<a href="https://github.com/apache/flink-cdc/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=apache/flink-cdc"/>
</a>
