;;; -*- Mode: Scheme; -*-

(in-module 'kno/profiling)

(use-module '{logger reflection kno/profile text/stringfmts varconfig})

(define %loglevel %notice%)

(module-export! '{profile!
		  profile-module!
		  module-getprocs module-getcalls
		  profile/report})
(module-export! '{profile->table  profiles->table})
(module-export! '{profile/user% profile/sys% profile/run% profile/idle%
                  profile/runsecs profile/idlesecs})

(define-init profile-root #f)
(varconfig! profile:root profile-root)

(define (module-getprocs module)
  (for-choices (sym (module-binds (get-module module)))
    (pick (importvar module sym) applicable?)))

(define (profile/runsecs profile)
  (+ (profile/utime profile) (profile/stime profile)))
(define (profile/idlesecs profile)
  (- (profile/time profile) (+ (profile/utime profile) (profile/stime profile))))

(define (profile/user% profile)
  (* 100 (/~ (profile/utime profile) (profile/time profile))))
(define (profile/sys% profile)
  (* 100 (/~ (profile/stime profile) (profile/time profile))))
(define (profile/run% profile)
  (* 100 (/~ (+ (profile/stime profile) (profile/utime profile))
             (profile/time profile))))
(define (profile/idle% profile)
  (* 100 (/~ (- (profile/time profile) 
                (+ (profile/stime profile) (profile/utime profile)))
             (profile/time profile))))

(define (invert-table table)
  (let ((result (make-hashtable)))
    (do-choices (key (getkeys table))
      (add! result (get table key) key))
    result))

(define-init profilefns
  [time profile/time
   etime profile/etime
   stime profile/stime
   utime profile/utime
   secs  profile/time
   runsecs profile/runsecs
   idlesecs profile/idlesecs
   run% profile/run%
   idle% profile/idle%
   user% profile/user%
   sys% profile/sys%
   calls profile/ncalls
   ndcalls profile/nitems
   waits profile/waits
   pauses profile/pauses
   switches profile/pauses
   faults profile/faults
   #t profile/time])

(define-init profilefn-names (invert-table profilefns))
(store! profilefn-names profile/time 'time)

(define (profname x)
  (try (get profilefn-names x) (or (procedure-name x) (lisp->string x))))

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
    (loginfo |Profiling|
      "The " (if (eq? root profile-root) "default ") "root " root
      " was not profiled! Ignoring it.")
    (set! root #f))
  (let* ((ranks (make-hashtable))
         (fn-names (invert-table profilefns))
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
    (lineout (if rootval (profname profilefn)) (if rootval "%") "\t" (profname profilefn)
      (dolist (fn morefns) (printout "\t" (profname fn))))
    (lineout "================================================================")
    (doseq (fcn (rsorted (getkeys ranks) ranks))
      (let ((info (profile/getcalls fcn)))
	(lineout (procedure-name fcn) "\t"
          (if rootval (printout "\t" (profname profilefn) "%" "=" (show% (profilefn info) rootval))) 
	  "\t" (profname profilefn) "=" (profilefn info)
	  (dolist (fn morefns)
            (printout "\t" (profname fn) "=" (fn info)))))
      (lineout "----------------------------------------------------------------"))))

;;; Profile function

(define (profile! fcn)
  (config! 'profiled fcn))

;;;; Profiles to tables

(define (profile->table profile)
  (let ((result (deep-copy profilefns)))
    (do-choices (key (getkeys profilefns))
      (store! result key ((get profilefns key) profile)))
    result))

(defambda (profiles->table (fcns (config 'profiled)) (root #f))
  (let ((results (frame-create #f))
        (root (and root (profile/getcalls root))))
    (do-choices (fcn fcns)
      (let* ((profile (profile/getcalls fcn))
             (table (profile->table profile)))
        (when root
          (store! profile '%time (/ (* 100.0 (profile/time profile))
                                    (profile/time root)))
          (store! profile '%etime (/ (* 100.0 (profile/etime profile))
                                     (profile/etime root)))
          (store! profile '%calls (/ (* 100.0 (profile/ncalls profile))
                                     (profile/ncalls root)))
          (store! profile '%waits (/ (* 100.0 (profile/waits profile))
                                     (profile/waits root)))
          (store! profile '%pauses (/ (* 100.0 (profile/pauses profile))
                                      (profile/pauses root)))
          (store! profile '%faults (/ (* 100.0 (profile/faults profile))
                                      (profile/faults root))))
        (store! results
            (if (procedure? fcn)
                (or (procedure-name fcn) (lisp->string fcn))
                fcn)
          (profile->table (profile/getcalls fcn)))))
    results))

