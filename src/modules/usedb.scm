;;; -*- Mode: Scheme; Character-Encoding: utf-8; -*-

(in-module 'usedb)

;;; This provides a layer for accessing database configurations.
;;;  Currently, it just allows for configurations specifying pools
;;;  and indices, but the intent is to keep information relevant to
;;;  journalling and syncing in this same data structure.

(define version "$Id$")

(module-export! 'usedb)

(define (usedb name)
  (let ((dbname (cond ((file-exists? name) name)
		      ((file-exists? (stringout name ".db"))
		       (stringout name ".db"))
		      (else (error "No such database " name)))))
    (let ((dbdata (file->dtype dbname)))
      (do-choices (pool (get dbdata 'pools))
	(add! dbdata '%pools
	      (cond ((not (string? pool)))
		    ((position #\@ pool) (use-pool pool))
		    ((has-prefix pool "/") (use-pool pool))
		    (else (use-pool (get-component pool dbname))))))
      (do-choices (index (get dbdata 'indices))
	(add! dbdata '%indices
	      (cond ((not (string? index)))
		    ((position #\@ index) (use-index index))
		    ((has-prefix index "/") (use-index index))
		    (else (use-index (get-component index dbname))))))
      (do-choices (config (get dbdata 'configs))
	(cond ((not (pair? config)))
	      ((and (pair? (cdr config)) (eq? (cadr config) 'FILE))
	       (config! (car config) (get-component (third config) dbname)))
	      (else (config! (car config) (cdr config)))))
      (let ((mi (get dbdata 'metaindex))
	    (rmi (make-hashtable)))
	(do-choices (slotid (getkeys mi))
	  (let ((index (get mi slotid)))
	    (add! rmi slotid
		  (cond ((not (string? index)))
			((position #\@ index) (use-index index))
			((has-prefix index "/") (use-index index))
			(else (use-index (get-component index dbname)))))))
	(add! dbdata '%metaindex rmi))
      dbdata)))





