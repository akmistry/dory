# Dory
A cache utilising spare/unused memory.

# Background
Dory was inspired by looking at resource utilisation within clusters.
Generally, when tasks are launched in a cluster (i.e. on Kubernetes), they are
provisioned with more memory than will be used on a regular basis. A HTTP
server might be provisioned for 128MiB of memory, but only uses 32MiB 99% of
the time, increasing and decreasing as load varies. The provisioned memory
amount is usually the peak that the server is expected to use.

In a desktop or traditional server, that unused memory will be used by the page
cache to keep pages from disk in memory. This improves performance by avoiding
slow disk accesses (even for SSDs). However, in a distributed system, the page
cache is significantly less effective due to the use of remote/distributed
storage systems instead of local disk.

So the idea is to try and use that unused memory for a cache, but with the
ability to raidly return that memory to the OS when needed.

# Design
Before discussing design, it's important to talk about non-goals for Dory.
Specifically:
- NOT designed for ultra high performance
- NOT designed for consistency
- NOT designed to be memory efficient

Although these are non-goals, it's important to be reasonable. Memory overhead
is on the order of ~100 bytes/key-value pair. Currently, ~50% of CPU usage is
in the protocol stack, which is a very minimal implementation of the redis
protocol.

That said, the single primary design goal is to be able to rapidly return
memory to the OS. This is mainly achieved by avoid the heap as much as
possible. Although Go has an excellent garbage collector, factors such as heap
fragmentation, a probelm which exists in every language and runtime, prevent
memory from being returned to the OS in a deterministic manner.

Instead, anonymous mmap'd areas are created, and managed using a structure
called PackedTable (naming is hard). PackedTable implements a key-value table
while keeping the key/value data in a single \[\]byte buffer, backed by the
mmap. The structure is similar to an SSTable, where the key/values are stored
contiguously, with an index stored in a Go map. When memory needs to be
returned to the OS, the mmap'd area is simply unmapped.

At a higher level, the Memcache holds a list of PackedTables and a map of keys
to PackedTables. The indirection in Memcache allows for any number of
PackedTables to disappear at any point in time. The Memcache also monitors the
level of free memory by polling /proc/meminfo and adjusting the number of
PackedTables as necessary.

# Usage

The `dory` directory contains the server. By default, dory listens on port
6379, since it implements a small subset of the redis protocol.

Dory only implements the following redis commands:
- SET
- GET
- DEL
- EXISTS

The protocol has been tested with `redis-benchmark`, `redis-cli` and
the [go-redis](https://github.com/redis/go-redis) client library.

The ideal way to deploy dory would be as a DaemonSet on kubernetes. A single
instance on every node will use up any available unused memory on the node.
However, work needs to be done on a client library to make this feasible.

# Licence

Dory is released under the Apache 2.0 license
