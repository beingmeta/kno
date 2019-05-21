;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'usedb)

;;; This provides a layer for accessing database configurations.
;;;  Currently, it just allows for configurations specifying pools
;;;  and indexes, but the intent is to keep information relevant to
;;;  journalling and syncing in this same data structure.

(use-module '{readfile flexdb texttools})

(module-export! '{usedb use-component})

(define (use-component usefn name dbname (opts #f) (warn #t))
  (cond ((not (string? name)))
	((position #\@ name)
	 (onerror (usefn name)
	   (lambda (ex)
	     (when warn
	       (warning "Unexpected error accessing " name " from " dbname ": "
			ex))
	     #f)
	   (lambda (v) (fail))))
	((has-prefix name "/")
	 (if (file-exists? name)
	     (if opts
		 (usefn name opts) 
		 (usefn name))
	     (prog1 (fail)
	       (when warn
		 (warning "Can't access file " name " from " dbname)))))
	(else
	 (if (file-exists? (get-component name dbname))
	     (if opts 
		 (usefn (get-component name dbname) opts)
		 (usefn (get-component name dbname)))
	     (prog1 (fail)
	       (when warn
		 (warning "Can't access file " name " from " dbname)))))))

(define (usedb spec (opts #f) (loc))
  (cond ((table? spec)
	 (let ((dbdata (deep-copy spec))
	       (root (getopt spec 'root (getopt opts 'root (getcwd)))))
	   (do-choices (pool (get dbdata 'pools))
	     (add! dbdata '%pools (flexdb/ref (mkpath root pool) opts)))
	   (do-choices (index (get dbdata '{indexes indices}))
	     (add! dbdata '%indexes (flexdb/ref (mkpath root index) opts)))
	   (do-choices (config (get dbdata 'configs))
	     (cond ((not (pair? config)))
		   ((and (pair? (cdr config)) (eq? (cadr config) 'FILE))
		    (config! (car config) (get-component (third config) root)))
		   (else (config! (car config) (cdr config)))))
	   (when (test dbdata '{metaindex slotindex})
	     (let ((mi (get dbdata '{metaindex slotindex}))
		   (rmi (make-hashtable)))
	       (do-choices (slotid (getkeys mi))
		 (let ((index (get mi slotid)))
		   (add! rmi slotid 
			 (if (string? index)
			     (usedb (mkpath root index) opts)
			     (flexdb/ref index)))))
	       (add! dbdata '%metaindex rmi)))
	   dbdata))
	((not (string? spec)) (irritant name |BadDBspec|))
	((and (file-exists? spec) (string-ends-with? spec #("." (* (isalpha)) {"pool" "index"}))) 
	 (flexdb/ref spec opts))
	((has-suffix spec {".db" ".dbspec" ".dtype" ".lsd" ".scd"})
	 (usedb (readfile spec) (opts+ opts `#[root ,(dirname spec)])))
	((file-directory? spec)
	 (let ((files (getfiles spec)))
	   (usedb `#[pools ,(pick files string-ends-with? #("." (* (isalpha)) "pool"))
		     indexes ,(pick files string-ends-with? #("." (* (isalpha)) "index"))
		     configs ,(for-choices (config (pick files has-suffix ".xcfg"))
				(cons (string->symbol (downcase (basename config ".xcfg")))
				      (readfile config)))])))
	((exists file-exists? (glom spec {".db" ".dbspec" ".dtype" ".lsd" ".scd"}))
	 (usedb (smallest (pick (glom spec {".db" ".dbspec" ".dtype" ".lsd" ".scd"}) file-exists?))
		(opt+ opts `#[root ,(dirname spec)])))
	((file-exists? (glom spec ".pool")) (usedb (glom spec ".pool")))
	((file-exists? (glom spec ".index")) (usedb (glom spec ".index")))
	(else (irritant spec |BadDBspec| ))))

