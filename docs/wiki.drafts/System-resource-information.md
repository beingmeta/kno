# Counting Resources

The procedure `(resources)` returns a slotmap containing various
implementation-dependent resource information, e.g.

    
    
        #|kno>|# (resources)
        #[MEMORY: 688 SWAPS: 0 USER-USECS: 57584 SYSTEM-USECS: 103456
        CONSES: 746 MALLOCD: 264 CONS-MEMORY: 12232 REFERENCED-OIDS: 0
        LOADED-OIDS: 0]
      
    

The function `GET` can be used to extract fields from a slotmap, E.G.

    
    
        #|kno>|# (get (resources) 'cons-memory)
        167218
      
    

The `(clock)` function returns the number of microseconds of processing time
expended since the first time `clock` was called:

    
    
        #|kno>|# (clock)
        0
        #|kno>|# (clock)
        1652000
      
    

The `(memusage)` function returns the number of KBytes of memory being used by
the data of the current process. This is based on the operating system's
accounting.

The `(consusage)` function returns the number of bytes of memory being used by
the current process. This uses Kno's own accounting methods rather than
the operating systems and also leaves out conses which have been allocated but
are not currently being used.

