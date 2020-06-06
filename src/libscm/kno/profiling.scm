;;; -*- Mode: Scheme; -*-

(in-module 'kno/profiling)

(use-module '{logger reflection})

(define %loglevel %notice%)

(module-export! '{profile-module! module-getprocs module-getcalls})

(define (module-getprocs module)
  (for-choices (sym (module-bindings (get-module module)))
    (pick (importvar module sym) applicable?)))

(define sortfns
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
		    (get sortfns sort)
		    (irritant sort |BadCallSortFn|)))
      (reject (profile/getcalls (module-getprocs module)) profile/ncalls 0)))

(define (profile-module! module)
  (let ((procs (module-getprocs module)))
    (config! 'profiled procs)
    (|| procs)))

