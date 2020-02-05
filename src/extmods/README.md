This directory (`src/extmods`) contains KNO cmodules which are maintained in separate repositories,
set up as GIT submodules. The commands `domake` and `submake` provide handy ways to operate on these
subdirectories. Each takes a series of either submodule names (which are subdirectories of
`src/extmods`) and make targets (anything else). It then iterates over the specified submodules and
calls make in each subdirectory with the specified targets.

The difference between `submake` and `domake` is that submake passes DEFINEs to the make call which
cause it to refer to the current build. This includes specifying a variant version of `knoconfig`
which gets headers and libraries from the current (enclosing) source tree and a `COPY_CMODULES`
definition which causes built modules to be stored in the source tree's `lib/kno` directory.

