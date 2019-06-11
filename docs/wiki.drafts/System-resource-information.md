# Counting Resources

The procedure `(rusage)` returns a slotmap containing various
implementation-dependent resource information, e.g.
````console
#|kno>|# (rusage)
#[MEMORY: 688 SWAPS: 0 USER-USECS: 57584 SYSTEM-USECS: 103456
  ]
````

With an argument, `RUSAGE` gets a particular field:
````console
#|kno>|# (get (resources) 'cons-memory)
167218
````

The `(clock)` function returns the number of microseconds of processing time
expended since the first time `clock` was called:
````console
        #|kno>|# (clock)
        0
        #|kno>|# (clock)
        1652000
````

The `(memusage)` function returns the number of KBytes of memory being
used by the data of the current process. This is based on the
operating system's accounting.

