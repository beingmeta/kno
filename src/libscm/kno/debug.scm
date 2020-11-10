;;;; -*- Mode: Scheme; -*-

(in-module 'kno/debug)

(use-module '{reflection texttools stringfmts logger})

(module-export! '{debug.command backtrace.command 
		  bt.command frame.command
		  getframe.command getenv.command})

(define (set-debug! (arg #f))
  (unless arg
    (set! arg (req/get '_debug (req/get '_err #f))))
  (cond ((not arg)
	 (logwarn |NoException| "No current or past exception to debug!"))
	((req/test '_debug arg))
	(else
	 (logwarn |Debugging| (exception-summary arg) 
		  "\n" (timestamp+ (exception-timebase arg) (exception-moment arg))
		  " (elapsed=" (exception-moment arg) ")"
		  "\nthread #" (exception-threadno arg) " in session " (exception-sessionid arg))
	 (req/set! '_debug arg)
	 (req/set! '_debug_stack (exception-stack arg))
	 (req/set! '_debug_stack_entry #f)))
  arg)

(define (debug.command (arg (req/get '_debug (req/get '_err #f))))
  (if arg (set-debug! arg)
      (logwarn |NoError| "Nothing to debug")))

(define (display-stackframe frame)
  (let ((fcn (stack-function frame))
	(args (stack-args frame))
	(env (stack-env frame)))
    (lineout (stack-depth frame) " " (stack-label frame)
      (if (and (stack-origin frame)
	       (not (equal? (stack-origin frame) (stack-label frame))))
	  (printout "/" (stack-origin frame)))
      (if (or (applicable? fcn) (special-form? fcn) (macro? fcn))
	  (printout " " (or (procedure-name fcn) fcn)))
      (if (stack-filename frame) (printout " " (write (stack-filename frame))))
      (if args (printout " " ($count (length args) "arg")))
      (if env (printout " binding"
		(do-choices (sym (getkeys env) i)
		  (printout (if (> i 0) ",") " " sym)))))))

(define (getframe.command (n #f))
  (when (set-debug!)
    (let* ((stack (req/get '_debug_stack))
	   (depth (length stack)))
      (if (or (>= n depth) (< n 0))
	  (logwarn |StackRangeError|
	    "The specified start frame " base " is outside of the stack (" depth " frames)")
	  (elt stack (- depth 1 n))))))
(define (getenv.command (n #f))
  (when (set-debug!)
    (let* ((stack (req/get '_debug_stack))
	   (depth (length stack)))
      (if (or (>= n depth) (< n 0))
	  (logwarn |StackRangeError|
	    "The specified start frame " base " is outside of the stack (" depth " frames)")
	  (stack-env (elt stack (- depth 1 n)))))))

(define (frame.command (n #f))
  (when (set-debug!)
    (when (and (not n) (req/test '_debug_stack_entry))
      (set! n (stack-depth (req/get '_debug_stack_entry))))
    (let* ((stack (req/get '_debug_stack))
	   (depth (length stack)))
      (if (or (>= n depth) (< n 0))
	  (logwarn |StackRangeError|
	    "The specified start frame " base " is outside of the stack (" depth " frames)")
	  (let* ((frame (elt stack (- depth 1 n)))
		 (args (stack-args frame))
		 (env (stack-env frame))
		 (source (stack-source frame))
		 (source-context (stack-annotated-source frame)))
	    (req/set! '_debug_stack_entry stack)
	    (display-stackframe frame)
	    (unless (= (length args) 0)
	      (doseq (arg args i)
		(lineout "arg" i "\t" (listdata arg))))
	    (when env
	      (let ((vars (getkeys env)))
		(lineout ($count (|| vars) "binding") ":")
		(do-choices (key vars)
		  (let* ((val (get env key)) (string (stringout (write val))))
		    (cond ((and (not (multiline-string? string)) (< (length string) 45))
			   (lineout " " key "\t" string))
			  ((ambiguous? val)
			   (lineout " " key ":")
			   (do-choices (v val)
			     (lineout "    "
			       (indent-text (stringout (listdata v)) 4))))
			  (else
			   (lineout " " key ":")
			   (lineout "  "
			     (indent-text (stringout (listdata val)) 2)))))))))))))

(define (backtrace.command (n #f) (base 0))
  (when (set-debug!)
    (let* ((stack (req/get '_debug_stack))
	   (depth (length stack)))
      (if (>= base depth)
	  (logwarn |StackRangeError|
	    "The specified start frame " base " is beyond the end of the stack")
	  (doseq (frame (reverse (slice stack base (and n (+ base (min n depth))))))
	    (display-stackframe frame))))))
(define bt.command (fcn/alias backtrace.command))
