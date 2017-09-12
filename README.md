# Dory
A cluster-wide cache utilising spare/unused memory.

# Background
Dory was inspired by looking at resource utilisation within clusters. Generally, when tasks are launched in a cluster, they
are allocated more memory than will be used on a regular basis. A HTTP server might be allocated 128MiB of memory, but
only uses 32MiB 99% of the time, increasing and decreasing as load varies.

In a desktop or traditional server, that unused memory will be used by the page cache to keep pages from disk in memory. This
improves performance by avoiding slow disk accesses (even for SSDs). However, in a distributed system, the page cache is
significantly less effective due to the use of remote/distributed storage systems instead of local disk.

As for the name, Dory forgets... quickly...

# Design
Before discussing design, it's important to talk about non-goals for Dory. Specifically:
- NOT designed for high performance
- NOT designed for strong consistency
- NOT designed to be memory efficient

Although these are non-goals, it's important to be reasonable. Memory overhead is ~32 bytes/key-value pair. Currently, ~90%
of CPU usage is in the protocol stack. Significant improvements to performance can come from either changes
to Go's HTTP2 stack (ideal), or switching to a different wire protocol. A single server provides sequential consistency,
although no CAS-like operations are supported (intentionally).

That said, the single design goal is to be able to rapidly return memory to the OS. This is mainly achieved by avoid Go's
heap as much as possible. Although Go has an excellent garbage collector, factors such as heap fragmentation prevent
memory from being returned to the OS in a deterministic manner.

The core structure where data is stored is the PackedTable (naming is hard). PackedTable implements a key-value table while
keeping the key/value data in a single \[\]byte buffer. The structure is similar to an SSTable, where the key/values are
stored contiguously, with an index stored in a Go map. The \[\]byte buffer used by PackedTable is explictly mmap'd, and
munmap'd when freed.

At a higher level, the Memcache holds a list of PackedTables and a map of keys to PackedTables. The indirection in Memcache
allows for any number of PackedTables to disappear at any point in time. The Memcache also monitors the level of free memory
by polling /proc/meminfo and adjusting the number of PackedTables as necessary.
