# Fault-tolerant dataflow platform
Project for the "Distributed Systems" course at Politecnico di Milano made by Toschi, Giannubilo and Galimberti - AY 2023/2024

## Description
The objective of this project is to implement a distributed dataflow platform for processing large amount (big-data) of keyvalue pairs, where keys and values are integers.
The platform is capable of running programs composed of a combination of four operators:
- map *(f: int → int)*: for each input tuple *<k, v>*, it outputs a tuple *<k, f(v)>*
- filter *(f: int → boolean)*: for each input tuple *<k, v>*, it outputs the same tuple *<k, v>* if *f(v)* is true, otherwise it drops the tuple
- changeKey *(f: int → int)*: for each input tuple *<k, v>*, it outputs a tuple *<f(v), v>*
- reduce *(f: list<int> → int)*: takes in input the list *V* of all values for key *k*, and outputs a single tuple *<k, f(V)>* (the reduce operator, if present in a program, is always the last one)

The platform includes a coordinator and multiple workers running on multiple nodes of a distributed system.
The coordinator accepts dataflow programs specified as an arbitrarily long sequence of the above operators. For instance, programs may be defined in JSON format and may be
submitted to the coordinator as input files. Each operator is executed in parallel over multiple partitions of the input data, where the number of partitions is specified as part of the dataflow program.
The coordinator assigns individual tasks to workers and guides the computation.

Decide the architecture of the application, the channels to use (distributed file system vs
network channels) and the approach to processing (batch vs stream) to optimize
performance, considering all the aspects that may impact the execution time, including the
need to pass data around. Implement also fault-tolerance mechanisms that limit the amount
of work that needs to be re-executed in the case a worker fails.
The project can be implemented as a real distributed application (for example, in Java) or it can be simulated using OMNet++.

## Assumptions
- Workers may fail at any time, while we assume the coordinator to be reliable.
- Network links, when present, are reliable (use TCP to approximate reliable links and
assume no network partitions can occur). The same is true for the storage of each
node, if present and used.
- You may implement either a scheduling or a pipelining approach. In both cases,
intermediate results are stored only in memory and may be lost if a worker fails. On
the other end, nodes can rely on some durable storage (the local file system or an
external store) to implement fault-tolerance mechanisms.
- You may assume input data to be stored in one or more csv files, where each line
represents a *<k, v>* tuple.
- You may assume that a set of predefined function exists and they can be referenced
by name (for instance, function ADD(5) is the function that takes in input an integer x
and returns integer x+5)

## Our idea
- Batch approach for the data
- Scheduling approach for the operations
- Local file system for persisting information
- Full mesh topology
- Fully simulated on OMNeT++
- One leader node and a parametrized number of worker nodes
- The reduce operation performs an addition
