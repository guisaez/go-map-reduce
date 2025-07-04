# MAP REDUCE

This is my implementation of the MapREduce assignment from the MIT 6.824 Distrbuted Systems course.
The project demostrates a simplified version of the MapReduce framework similar to Google's original
system, implemented in Go.

## Overview

This project consists of:

* A Coordinator that manages task distribution.
* Multiple Workers that perform `map` and `reduce` tasks.
* A fault tolerant design that reassigns tasks if a worker fails or times out.

The system processes input files to produce key/value pairs using the `map` function,
groups them by key, and applies a `reduce` function to aggregate results.

### How it Workes

1. The Coordinator:
  * Starts and waits for workers to register.
  * Assigns `map` tasks
  * Waits for all map tasks to complete.
  * Assigns `reduce` tasks.
  * Waits for all reduce tasks to complete.
2. Each Worker:
  * Requests a task.
  * Executes the  `map` or `reduce` function.
  * Reports task completiom
  * Retries if assigned a new task due to failure

### Project structure

```
.
├── cmd/
│   ├── mrcoordinator/main.go   # Coordinator initializer
│   ├── mrworker/main.go        # Worker initializer
│   ├── mrsequential/main.go    # Sequential coordinator/worker initializer
├── /
│   ├── mrcoordinator.go        # Coordinator implementation
│   ├── mrworker.go             # Worker implementation
│   ├── rpc.go            # Shared definitions and RPC structs
│   └── test-mr.sh              # Test script (provided by MIT, but modified by to improve file organization)
├── input/
│   ├── pg-being_ernest.txt                   # Word count plugin
├── mrapps/
│   ├── wc.go                   # Word count plugin
│   ├── indexer.go              # Inverted index plugin
├── test/
│   └── test-mr.sh              # Test script (provided by MIT, but modified by to improve file organization)
```

#### Getting started

##### Running the Word Count Example

```
make coordinator
```

In a separate terminal, run one or more workers

```
make worker
```
```
make spawn_worker
```

#### Features

* Fault-tolerant worker management
* Task timeout and reassignment
* Plugin system for map/reduce logic
* DEterministic partitioning for reducers