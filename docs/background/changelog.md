### [0.0.1](https://github.com/sparkutils/flow/milestone/1?closed=1) Initial Release <small>1st May, 2026</small>

The initial release of Flow supports the processing of a multi-root DAG of Steps, each performing a Quality 
transformation, or DQ check, of some data.

This release is built on Spark 4/4.1 builds of Quality 0.2.0, with the two Quality data models being supported 
(de-normalised and CombinedRuleSuiteRows) for rule maintenance allowing Flow to serialize to json configuration files 
suitable for publishing between development environments as well as a more gui friendly format.