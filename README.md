# Scrolls

## Overview

This is a prototype implementation of the Scrolls indexer with several new features.

### 1. Integration with the Dolos data node

The Dolos source stage from Oura was integrated into this implementation enabling fast data querying. More importantly, integration with Dolos enables efficient rollbacks through "undo block" events. This means the indexer can process blocks at the tip of the chain and maintain database integrity when rollbacks occur.

**Note:** Dolos is still missing the utxo-input-as-output feature aka "enriched blocks" which is very important for this implementation to be fully functional.

### 2. Ability to write reducers in Typescript / Javascript

This implementation uses Rust's `deno_runtime` library to load reducers as Javascript plugins. Reducers describing block transformations can be written in Typescript, bundled into Javascript and loaded into Scrolls. This decouples the Scrolls codebase from the reducer codebase and has several benefits. 

1. Scrolls no longer has to be recompiled to add new reducers
2. Typescript / Javascript is a much more accessible language for developers enabling reducers to be written by a larger community

Ultimately, the Scrolls codebase is relegated to feeding data to the transformation logic and storing the processed results.

### 3. Ability to store data in either a Redis or a SQL database

Reducers have the option of outputting two different type of storage events:

1. *Conflict-free Replicated Data Types (CRDTs) Command* 

    These events are consumed by Redis (or kvrocks).

2. *Relation Database Management System (RDBMS) Command*

    These events are consumed by relational databases such as Postgres or MySQL.

## Try it out!

Two sets of reducers have been written as templates in the `examples/` folder demonstrating both data storage event types. The reducers use the [cardano-multiplatform-lib](https://github.com/dcSpark/cardano-multiplatform-lib/tree/develop) to parse addresses and stake addresses from bytes. 

*Note that the reducers will not currently compute correct balances as we still require the Dolos utxo-input-as-output feature.*

## Future work

This tool will remain under heavy development as we work to productionize it. Eventually we hope to merge these features into the main [Scrolls repo](https://github.com/txpipe/scrolls).
