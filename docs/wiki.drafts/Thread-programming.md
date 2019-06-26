Kno provides extensive support for the implementation of multi-
threaded applications. Multi-threaded applications can do many things at once,
proceeding with one task while blocked on another. On machines with multiple
processors, different tasks can be divided among the different processors,
possibly leading to performance improvements over performing all of the tasks
on a single processor.

The support for multi-threaded programming in Kno is provisional. The
chief constructs for starting multiple independent threads are `PARALLEL` and
`SPAWN`.

`(parallel expri...)`

    Evaluates each expri in a separate thread, combining the returned result choices into a single set of choices. In the absence of side effects (including I/O), this is just equivalent to `AMB`.
`(spawn expri...)`

    Evaluates each expri in a separate thread, but returns immediately and discards any results returned by the individual expressions.
`(make-mutex)`

    Returns a "mutex object" which can be used to make sure that separate threads do not interfere when accessing shared resources.
`(with-mutex-locked mutex-expr expri....)`

    Evaluates mutex-expr and then evaluates each of the expri.... expressions while guaranteeing that no other thread will evaluate a `with-mutex-locked` expression referring to the same value of mutex-expr.

## Synchronized Procedures

Kno also provides _synchronized procedures_. A procedure returned by `
SLAMBDA` (which is syntatically identical to `LAMBDA`) or defined by `SDEFINE`
(which is syntactically identical to `DEFINE`) is guaranteed to be running in
only one thread at any moment.

For example, the following server initialization (.fdz) file uses a
synchronized lambda to control writing to a data file even when running on a
multi-threaded server (by default, Kno servers are multi-threaded on
platforms where `configure` can figure out how to compile them thus).

    
    
            ;; This is the file fdlog.fdz
            (sdefine (log x)
      (add-dtype-to-file x "log.dtype"))
    

This is also an example of a "safe" wrapper around a potentially dangerous
function (`add-dtype-to-file`). External clients can call the defined `log`
procedure, but cannot call `add-dtype-to-file` directly (which writes to the
local filesystem).

