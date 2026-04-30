# Flow - {{project_version()}}

??? coverage "Coverage"
    
    <table>
    <tr>
        <td>Statement</td>
        <td class="coveragePercent">{{statement_coverage()}}:material-percent-outline:</td>
        <td>Branch</td>
        <td class="coveragePercent">{{branch_coverage()}}:material-percent-outline:</td>
    </tr>
    </table>

## Process a Graph of data processing Steps with Quality 

Write auditable DQ and transformation rules using simple SQL or create re-usable functions via SQL Lambdas and manage them through a graph of Steps.

Your Flows can be provided DataFrames from either a Quality DataFrameLoader, catalog or even custom loading logic.

* :new:{.pulseABit} Initial Release

Entire Flows are lazily evaluated but support caching of a Steps results for interim performance benefits.

Each Step in a Flow has an associated Runner and, unless configured not to, keeps an audit of all the results of the previous Steps in the graph,
either Quality:

1. engine,
2. collector,
3. folder,
4. DQ or 
5. a custom implementation

The resulting Column then has a ResultApproach applied to it, either:

* AsIs - the audit and result column is added to the DataFrame,
* MergeFields - any nested result fields are merged with the DataFrame,
* OutputFieldsOnly - only the audit column and nested result columns are output,
* StarOnly - the audit column is dropped and only nested result columns are output,
* OutFieldOnly - only the result column remains or
* a custom implementation
