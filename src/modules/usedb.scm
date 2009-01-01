;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'usedb)

;;; This provides a layer for accessing database configurations.
;;;  Currently, it just allows for configurations specifying pools
;;;  and indices, but the intent is to keep information relevant to
;;;  journalling and syncing in this same data structure.

(define version "$Id: usedb.scm 2694 2008-05-29 12:09:01Z haase $")

(module-export! 'usedb)

(define (use-component usefn name dbname (warn #t))
  (cond ((not (string? name)))
	((position #\@ name)
	 (onerror (usefn name)
		  (lambda (ex)
		    (when warn
		      (warning "Unexpected error accessing " name " from " dbname ": " ex))
		    #f)
		  (lambda (v) (fail))))
	((has-prefix name "/")
	 (if (file-exists? name)
	     (usefn name)
	     (prog1 (fail) (when warn (warning "Can't access file " name " from " dbname)))))
	(else
	 (if (file-exists? (get-component name dbname))
	     (usefn (get-component name dbname))
	     (prog1 (fail) (when warn (warning "Can't access file " name " from " dbname)))))))

(define (usedb name)
  (let ((dbname (cond ((file-exists? name) name)
		      ((file-exists? (stringout name ".db"))
		       (stringout name ".db"))
		      (else (error "No such database " name)))))
    (let ((dbdata (file->dtype dbname)))
      (do-choices (pool (get dbdata 'pools))
	(add! dbdata '%pools (use-component use-pool pool dbname)))
      (do-choices (index (get dbdata 'indices))
	(add! dbdata '%indices (use-component use-index index dbname)))
      (do-choices (config (get dbdata 'configs))
	(cond ((not (pair? config)))
	      ((and (pair? (cdr config)) (eq? (cadr config) 'FILE))
	       (config! (car config) (get-component (third config) dbname)))
	      (else (config! (car config) (cdr config)))))
      (let ((mi (get dbdata 'metaindex))
	    (rmi (make-hashtable)))
	(do-choices (slotid (getkeys mi))
	  (let ((index (get mi slotid)))
	    (add! rmi slotid (use-component open-index index dbname #f))))
	(add! dbdata '%metaindex rmi))
      dbdata)))





