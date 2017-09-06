;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/registry)

;;; Maintaining registries of objects (OIDs) with unique IDs

(use-module '{ezrecords logger varconfig})

(define %used_modules 'ezrecords)

(module-export! '{use-registry set-registry!
		  register registry/ref
		  registry/check registry/errors
		  registry/repair!
		  registry/save!})

(module-export! '{registry-slotid registry-spec
		  registry-pool registry-index registry-server})

(define %loglevel %warn%)

(define (registry->string r)
  (stringout "#<REGISTRY " (registry-slotid r) " " 
    (write (pool-id (registry-pool r))) " "
    (write (index-id (registry-index r))) ">"))

(define-init registries (make-hashtable))

(defrecord (registry OPAQUE `(stringfn . registry->string))
  ;; TODO: Check for duplicate fields in defrecord
  slotid server pool index 
  (cache (make-hashtable))
  (lock (make-condvar)))
(module-export! '{registry? registry-pool registry-index registry-server})

(define-init registry
  (slambda (slotid pool index (server #f))
    (unique-registry slotid pool index server)))

(define (unique-registry slotid pool index (server #f))
  (let ((registry (get registries slotid)))
    (cond ((fail? registry)
	   (let ((consed (cons-registry slotid server pool index)))
	     (store! registries slotid consed)
	     (add! registries pool consed)
	     (add! registries index consed)
	     consed))
	  ((and (eq? pool (registry-pool registry))
		(eq? index (registry-index registry)))
	   registry)
	  (else (irritant slotid |RegistryConflict| 
		  "The existing registry for " slotid " "
		  "is not compatible with the pool " (write (pool-id pool)) " "
		  "and the index " (write (index-id pool)) ", which are "
		  (pool-id (registry-pool registry)) " and " 
		  (index-id (registry-pool registry))
		  "respectively")))))

(defambda (register slotid value 
		    (inits #t) (defaults #f) (adds #f)
		    (registry-arg #f)
		    (reg))
  (unless registry-arg
    (do-choices slotid
      (unless (get registries slotid)
	(irritant slotid |No Registry| REGISTER
		  "No registry exists for the slot "
		  (write slotid)))))
  (for-choices slotid
    (set! reg (or registry-arg (get registries slotid)))
    (info%watch "REGISTER" slotid value reg)
    (for-choices value
      (cond ((and reg (not defaults) (not adds))
	     ;; Simple call, since we dont' need to do anything 
	     ;; with the object after we get it.
	     (try (get (registry-cache reg) value)
		  (registry/get reg slotid value inits)))
	    (reg
	     (let ((frame 
		    (try (get (registry-cache reg) value)
			 (registry/get reg slotid value inits))))
	       (when defaults
		 (when (test defaults '%id) 
		   (store! frame '%id (get defaults '%id)))
		 (do-choices (slotid (getkeys defaults))
		   (unless (test frame slotid)
		     (store! frame slotid (get defaults slotid)))))
	       (when adds
		 (do-choices (slotid (getkeys adds))
		   (add! frame slotid (get adds slotid))))
	       frame))
	    (else (fail))))))

(define (registry/ref slotid value (registry))
  (default! registry (try (get registries slotid) #f))
  (unless registry (set! registry (try (get registries slotid) #f)))
  (if registry
      (try (get (registry-cache registry) value)
	   (registry/get registry slotid value #f))
      (irritant slotid |No Registry| registry/ref
		"No registry exists for the slot " (write slotid))))

(define (registry/save! (r #f))
  (when (and (or (symbol? r) (oid? r)) (test registries r))
    (set! r (get registries r)))
  (cond ((not r)
	 (registry/save! 
	  (qc (pick (get registries (getkeys registries))
		    registry-server #f))))
	((ambiguous? r)
	 (thread/wait (thread/call registry/save! r)))
	((registry-server r)
	 (logwarn |RemoteRegistry|
	   "No need to save a remote registry")
	 #f)
	(else (let* ((pools (registry-pool r))
		     (indexes (registry-index r))
		     (adjuncts-map (get-adjuncts pools))
		     (adjuncts (get adjuncts-map (getkeys adjuncts-map)))
		     (dbs (pick {pools indexes adjuncts} {pool? index?})))
		(when (exists modified? dbs) 
		  (let ((threads (thread/call+ #[logexit #f] commit dbs))
			(started (elapsed-time)))
		    (Loginfo |SavingRegistry| r)
		    (if (exists? threads)
			(begin (thread/wait threads)
			  (lognotice |RegistrySaved| 
			    "Saved registry " r " in " (secs->string (elapsed-time started))))
			(logwarn |NoRegistry| "Couldn't get a registry to save"))))))))

;;; The meat of it

(define (registry/get registry slotid value (create #f) (server) (index))
  (default! server (registry-server registry))
  (default! index (registry-index registry))
  (info%watch "REGISTRY/GET" registry slotid value create server index)
  (if server
      (try (find-frames index slotid value)
	   (dtcall server 'register slotid value))
      (with-lock (registry-lock registry)
	(try (get (registry-cache registry) value)
	     (let* ((key (cons slotid value))
		    (existing (find-frames index slotid value))
		    (result (try existing
				 (tryif create
				   (flex/make (registry-pool registry)
				     '%id (list slotid value)
				     '%session (config 'sessionid)
				     '%created (timestamp)
				     slotid value)))))
	       (when (exists? result)
		 (when (fail? existing)
		   (index-frame index result 'has slotid)
		   (index-frame index result slotid value)
		   (when (table? create)
		     (do-choices (key (getkeys create))
		       (store! result key (get create key)))))
		 (store! (registry-cache registry) value result))
	       result)))))



