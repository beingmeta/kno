(in-module 'usedb)

(module-export! 'usedb)

(define (usedb name)
  (let ((dbname (cond ((file-exists? name) name)
		      ((file-exists? (stringout name ".db"))
		       (stringout name ".db"))
		      (else (error "No such database " name)))))
    (let ((dbdata (file->dtype dbname)))
      (do-choices (pool (get dbdata 'pools))
	(cond ((not (string? pool)))
	      ((position #\@ pool) (use-pool pool))
	      ((has-prefix pool "/") (use-pool pool))
	      (else (use-pool (get-component pool dbname)))))
      (for-choices (index (get dbdata 'indices))
	(cond ((not (string? index)))
	      ((position #\@ index) (use-index index))
	      ((has-prefix index "/") (use-index index))
	      (else (use-index (get-component index dbname))))))))



