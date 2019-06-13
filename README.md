# Combination.WorkQueues

Work queue primitives for the .NET Task Parallel Library (TPL).

## Contents of this library

### WorkQueue
Basic asynchronous work queue with a configurable paralellism.

### GroupedWorkQueue
Work queue where items are batched together based on a key.

## How to build

    dotnet test
    dotnet pack -c Release
