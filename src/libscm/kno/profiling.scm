;;; -*- Mode: Scheme; -*-

(in-module 'kno/profiling)

(use-module '{logger kno/reflect kno/profile text/stringfmts varconfig})

(define %loglevel %notice%)

(module-export! '{profile!
		  profile-module!
		  module-getprocs module-getcalls
		  profile/report})

(define-init profile-root #f)
(varconfig! profile:root profile-root)

(define (module-getprocs module)
  (for-choices (sym (module-binds (get-module module)))
    (pick (importvar module sym) applicable?)))

(define profilefns
  [nsecs profile/nsecs
   time profile/nsecs
   stime profile/stime
   utime profile/utime
   secs  profile/time
   calls profile/ncalls
   ndcalls profile/nitems
   waits profile/waits
   pauses profile/pauses
   faults profile/faults
   #t profile/nsecs])

(define (module-getcalls module (sort #f))
  (if sort
      (rsorted (module-getcalls module #f)
	       (try (tryif (applicable? sort) sort)
		    (get profilefns sort)
		    (irritant sort |BadCallSortFn|)))
      (reject (profile/getcalls (module-getprocs module)) profile/ncalls 0)))

(define (profile-module! module)
  (let ((procs (module-getprocs module)))
    (config! 'profiled procs)
    (lognotice |Profile| "Profiling " ($count (|| procs) "procedure") " in " module)
    (|| procs)))

(defambda (profile/report (root profile-root) (fcns (config 'profiled))
			  (profilefn profile/time) . morefns)
  (when (and (symbol? profilefn) (test profilefns profilefn))
    (set! profilefn (get profilefns profilefn)))
  (when (and root (not (profile/getcalls root)))
    (logwarn |Profiling|
      "The " (if (eq? root profile-root) "default ") "root " root
      " was not profiled! Ignoring it.")
    (set! root #f))
  (let* ((ranks (make-hashtable))
	 (rootinfo (and root (profile/getcalls root)))
	 (rootval (and rootinfo (profilefn rootinfo)))
	 (rootvals (and rootinfo (pair? morefns) #[])))
    (when rootvals 
      (dolist (fn morefns)
	(if (and (symbol? fn) (get profilefns fn))
	    (store! rootvals fn ((get profilefns fn) rootinfo))
	    (store! rootvals fn (fn rootinfo)))))
    (do-choices (fcn (config 'profiled))
      (let* ((info (profile/getcalls fcn))
	     (val (profilefn info)))
	(unless (or (fail? val) (not (number? val)) (zero? val))
	  (store! ranks fcn val))))
    (lineout "================================================================")
    (doseq (fcn (rsorted (getkeys ranks) ranks))
      (let ((info (profile/getcalls fcn)))
	(if rootval
	    (lineout (show% (profilefn info) rootval) 
	      "\t" (procedure-name fcn) " ("  (profilefn info) ")"
	      (dolist (fn morefns)
		(when (test rootvals fn)
		  (printout "\t" (procedure-name fn) "=" (show% (fn info) (get rootvals fn)) 
		    " (" (fn info) ")"))))
	    (lineout (profilefn info) "\t" (procedure-name fcn)
	      (dolist (fn morefns) (printout "\t" (fn info)))))))))

(define (profile! fcn)
  (config! 'profiled fcn))
