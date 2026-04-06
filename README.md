# Flow

A lightweight Spark 4.x data flow application template, with auditable projections, joins and transformations powered by Quality.

## Why Flow?

Quality provides a number of optimised rule engines but no opinion on how they should be leveraged. 

Flow _is_ opinionated and, using Quality's ViewLoading and Rule Engines, provides a way to structure Steps in processing 
data driven purely by data configuration.  Audit trails of decision-making, as with Quality, is a first class citizen, 
allowing audit trails to be combined from all parent Steps. 

Flow does not aim to replace orchestration tools such as Airflow, instead it provides a generic data ingestion pipeline
processing template and takes care of the stitching together or each step.

## What is Flow?

* Flows are constructed with Steps
* Steps form a dependency graph based on Futures
* Each Step has: 
  * A unique name within the Flow
  * A set of Step name dependencies
  * Input and Output view names (or tokens for loading)
  * A Quality RuleSuite with ViewLoader's
  * An Operation of a Quality Runner with configurable result processing
  * Audit trail columns from previous steps can be combined
 

* Flows have several extension points:
  * Data Loading:
    * Provide root Steps in the graph with DataFrames
    * Delegate loading of views/tokens to the Catalog
    * Directly manage loading of DataFrames via a Quality DataFrameLoader
  * Step processing:
    * Before a Step starts the DataFrame can be manipulated based on the Step
    * After a Step completes custom logic for saving can be provided, by default temp views are used
* Flow works with Spark 4 Connect and Classic handling 
* Flows can be serialised and loaded for all supported types and Operations  