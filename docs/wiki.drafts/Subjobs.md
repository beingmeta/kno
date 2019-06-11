# Subjogs

Kno programs can run other programs as subjobs and
read and write input from those subjobs.
  
A subjob is a separate process from the Kno interpreter with which the
interpreter interacts. Subjobs can be local subjobs (started as programs on
the same machine as the interpreter) or remote subjobs (started by connecting
to a remote socket across the Internet). Both of these are called subjobs
because the Kno process may send output to and read input from them.

The simplest sort of subjob is started with the `SYSTEM` procedure, which
executes a command on the local operating system. It takes no input (other
than its command line) and its output is just sent to the console directly.
The call to `SYSTEM` waits until the external program is done and then returns
the exit code of the program.

The `SYSTEM` procedure takes an argument list like those passed to `PRINTOUT`
and uses them to construct a command line. For example:

    
    
    #|kno>|# (define filename "test.fdx")
    #|kno>|# (system "chmod a+x " filename)
    1
    

The `OPEN-PROCESS` procedure starts a parallel subprocess. It's first argument
is the program to start and its remaining arguments are converted into strings
and passed to the program. `OPEN-PROCESS` starts the subprocess and
immediately returns a **subjob** which Kno process can interact with.
This interaction occurs through regular I/O function addressed to particular
ports associated with the process.

` (SUBJOB-INPUT subjob)` returns an output port which can be used to send
output to the subjob. `(SUBJOB-OUTPUT subjob)` returns an input port which can
be used to read the output of the subjob. Error messages from subjobs started
by `OPEN-PROCESS` are sent to the console.

The procedure `OPEN-PROCESS-E` is just like `OPEN-PROCESS` but uses its
initial argument to specify where error messages from the process should be
sent. If this first argument is a string, the error messages are sent to the
file named by the string; if the first argument is false `#F`, errors are sent
to a special stream which can be retrieved by the `SUBJOB-ERRORS` accessor. If
the first argument is anything else, errors are just sent to the console.

For example, this interaction shows Kno using an inferior Kno
process to evaluate expressions:

    
    
    #|kno>|# (define xx (open-process "Kno" "-"))
    ;; Nothing (void) was returned
    ;; Values changed (1): XX
    #|kno>|# (printout-to (subjob-input xx) '(+ 2 3 (* 4 5)) "\n")
    ;; Nothing (void) was returned
    #|kno>|# (readline (subjob-output xx))
    "25"
    

The accessor `SUBJOB-PID` returns the process ID of a created subjob. The
procedure `SUBJOB-CLOSE` terminates a running subjob; it's second argument,
when provided, indicates a signal with which the subjob will be closed via the
`kill()` function.

