;;; -*- Mode: Scheme; Text-encoding: latin-1; -*-

;;; This is a simple test of dbg/wait which has the program pause to allow a developer to attach a
;;; debugger to it.  This currently just checks the base case. To run it, just use knox to execute
;;; the script with a CONFIG setting of WAITFOR to some string (i.e. WAITFOR=godot). It should
;;; pause, provide the executable name and PID and then you can attach the debugger to that PID and
;;; look at the situation.

;;; This doesn't test any of the additional arguments to dbg/wait, whose signature is:

;;;    (dbg/wait *value* [*msgval*] [*breakbefore*] [*global*])

;;; *msgval* is evaluated and #f causes nothing to be done (no message, no waiting).
;;; *breakbefore* has the messages and breaking happen before the evaluation as well as afterwards.
;;; *global* has the breakpoints attempt to pause all the other threads by setting the u8_paused flag.

(define (main)
  (if (config 'WAITFOR)
      (dbg/wait (config 'WAITFOR))))
