;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc.  All rights reserved.

(in-module 'storage/registry)

;;; Maintaining registries of objects (OIDs) with unique IDs

(use-module '{ezrecords logger varconfig storage/flexpools})
(define %used_modules 'ezrecords)

(module-export! '{use-registry set-registry!
		  register registry/ref
		  registry/check registry/errors
		  registry/repair!
		  registry/save!})

(module-export! '{registry-slotid registry-spec
		  registry-pool registry-index registry-server})

(define %loglevel %notice%)

;; Not yet used
(define default-registry #f)

;; This determines whether the pool should use a bloom filter to
;; optimize registration
(define use-bloom #f)
(varconfig! registry:bloom use-bloom)

(define (registry->string r)
  (stringout "#<REGISTRY " (registry-slotid r) " " (registry-spec r) ">"))

(define-init registries (make-hashtable))

(defrecord (registry OPAQUE `(stringfn . registry->string))
  ;; TODO: Check for duplicate fields in defrecord
  slotid spec server pool index 
  (slotindexes {}) (bloom #f)
  (cache (make-hashtable))
  (lock (make-condvar)))
(module-export! '{registry? registry-pool registry-index registry-server})

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
			 (registry/get reg slotid value inits)))
		   (slotindexes (registry-slotindexes reg)))
	       (when defaults
		 (when (test defaults '%id) 
		   (store! frame '%id (get defaults '%id)))
		 (do-choices (slotid (getkeys defaults))
		   (unless (test frame slotid)
		     (store! frame slotid (get defaults slotid))
		     (if (overlaps? slotid slotindexes)
			 (index-frame (registry-index reg) frame slotid)
			 (if (test (pick slotindexes table?) slotid)
			     (index-frame (get slotindexes slotid)
				 frame slotid))))))
	       (when adds
		 (do-choices (slotid (getkeys adds))
		   (add! frame slotid (get adds slotid))
		   (if (overlaps? slotid slotindexes)
		       (index-frame (registry-index reg)
			   frame slotid (get adds slotid))
		       (if (test (pick slotindexes table?) slotid)
			   (index-frame (get slotindexes slotid)
			       frame slotid (get adds slotid))))))
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
	  (pick (get registries (getkeys registries))
		registry-server #f)))
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
	     (let* ((bloom (registry-bloom registry))
		    (key (cons slotid value))
		    (existing (tryif (or (not bloom) (bloom/check bloom key)) 
				(find-frames index slotid value)))
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
		   (when bloom (bloom/add! bloom key))
		   (when (table? create)
		     (do-choices (key (getkeys create))
		       (store! result key (get create key)))))
		 (store! (registry-cache registry) value result))
	       result)))))

;;; Registering registries

(defslambda (register-registry-inner slotid spec (replace #f))
  (when replace (drop! registries slotid))
  (try (get registries slotid)
       (if (table? spec)
	   (let ((server (and (getopt spec 'server)
			      (if (dtserver? (getopt spec 'server))
				  (getopt spec 'server)
				  (open-dtserver (getopt spec 'server)))))
		 (pool (try (pool/ref (getsource spec 'pool) spec) #f))
		 (index (try (index/ref (getsource spec 'index)) #f))
		 (registry #f))
	     (if (or server (not (getopt spec 'bloom use-bloom)))
		 ;; Server-based registries don't (currently) have
		 ;; bloom filters or idstreams
		 (set! registry
		       (cons-registry slotid spec server pool index
				      (getopt spec 'slotindex {})))
		 (set! registry
		       (cons-registry slotid spec server pool index 
				      (getopt spec 'slotindex {})
				      (and (getopt spec 'bloom use-bloom)
					   (get-bloom index slotid)))))
	     (store! registries slotid registry)
	     registry)
	   (irritant spec |InvalidRegistrySpec|))))

(define (getsource spec slot)
  (try
   (getopt spec slot {})
   (let ((server (getopt spec 'server)))
     (cond ((not server) {})
	   ((string? server) server)
	   ((dtserver? server) (dtserver-id server))
	   (else {})))
   (getopt spec 'source {})))

(define (register-registry slotid spec (replace #f))
  (if replace
      (let ((cur (get registries slotid))
	    (new (if (registry? spec) 
		     (if (and (registry-slotid spec)
			      (not (eq? slotid (registry-slotid spec))))
			 (irritant spec
			     |SingleSlotRegistry| |register-registry| 
			     "The registry " registry " is configured for the slot "
			     (registry-slotid spec) " which isn't " slotid)
			 spec)
		     (register-registry-inner slotid spec replace))))
	(if (fail? new)
	    (logcrit |RegisterRegistry|
	      "Couldn't create registry for " slotid 
	      " specified as " spec)
	    (unless (identical? cur new)
	      (lognotice |RegisterRegistry|
		"Registry for " slotid " is now "
		(if (registry-server new)
		    (dtserver-id (registry-server new))
		    (pool-id (registry-pool new)))))
	    new))
      (try (get registries slotid)
	   (let ((new (register-registry-inner slotid spec replace)))
	     (lognotice |RegisterRegistry|
	       "Registry for " slotid " is now "
	       (if (registry-server new)
		   (dtserver-id (registry-server new))
		   (pool-id (registry-pool new))))
	     new))))

(define (registry-opts arg)
  (cond ((table? arg) (fixup-opts arg))
	((index? arg) (registry-opts (strip-suffix (index-id arg) ".pool")))
	((pool? arg)  (registry-opts (strip-suffix (pool-id arg) ".index")))
	((not (string? arg)) (irritant arg |InvalidRegistrySpec|))
	((exists position {#\: #\@} arg) `#[server ,arg])
	(else `#[source ,arg])))

(define (fixup-opts opts (source))
  (default! source (getopt opts 'source))
  (when (string? source)
    (when (and (not (getopt opts 'server))
	       (exists position {#\: #\@} source))
      (store! opts 'server source))
    (unless (getopt opts 'pool)
      (store! opts 'pool source))
    (unless (getopt opts 'index)
      (store! opts 'index source)))
  opts)

(define (use-registry slotid spec)
  (info%watch "USE-REGISTRY" slotid spec)
  (when (string? spec) (set! spec (registry-opts spec)))
  (try (get registries slotid)
       (register-registry slotid spec)))

(define (need-replace? registry spec)
  (or (and (getopt spec 'server) (not (registry-server registry)))
      (and (registry-server registry) (not (getopt spec 'server)))
      (if (registry-server registry)
	  (or (eq? (getopt spec 'server) (registry-server registry))
	      (and (dtserver? (getopt spec 'server))
		   (equal? (dtserver-id (registry-server registry))
			   (dtserver-id (getopt spec 'server))))
	      (equal? (dtserver-id (registry-server registry))
		      (getopt spec 'server)))
	  (or (and (not (registry-bloom registry))
		   (getopt spec 'bloom use-bloom))
	      (and (registry-bloom registry)
		   (not (getopt spec 'bloom use-bloom)))
	      (not (equal? (use-pool (get spec 'pool))
			   (registry-pool registry)))
	      (not (equal? (open-index (get spec 'index))
			   (registry-index registry)))))))

(define (set-registry! slotid spec)
  (info%watch "SET-REGISTRY!" slotid spec)
  (when (string? spec) (set! spec (registry-opts spec)))
  (if (test registries slotid)
      (when (need-replace? (get registries slotid) spec)
	(register-registry slotid spec #t))
      (register-registry slotid spec #t)))

;;; Getting the ids for a bloom filter

(define (get-bloom index slotid (room #f) (error 0.000001))
  (lognotice |BloomInit| 
    "Initializing bloom filter for " slotid " in " index)
  (let* ((started (elapsed-time))
	 (keys (pick (getkeys index) slotid))
	 (n-keys (choice-size keys))
	 (bloom-size (or room (* 4 (max n-keys 100000))))
	 (filter (make-bloom-filter bloom-size error)))    
    (bloom/add! filter keys)
    (lognotice |BloomInit| 
      "Initialized bloom filter with "
      (choice-size keys) " items in " 
      (secs->string (elapsed-time started)) 
      ":\n    " filter)
    filter))


;;; Checking and repairing registries

(define (registry/check registry (opts #f))
  (let* ((index (registry-index registry))
	 (slot (getopt opts 'slotid (registry-slotid registry)))
	 (keys (pick (getkeys index) slot)))
    (prefetch-keys! index slot)
    (let ((trouble (filter-choices (key keys)
		     (ambiguous? (get index key)))))
      (if (fail? trouble) #f
	  (begin (logwarn |RegistryError| 
		   (choice-size trouble) " of the " (choice-size keys)
		   " in " registry " have ambiguous references")
	    (choice-size trouble))))))

(define (registry/errors registry (opts #f))
  (let* ((index (registry-index registry))
	 (slot (getopt opts 'slotid (registry-slotid registry)))
	 (keys (pick (getkeys index) slot)))
    (prefetch-keys! index slot)
    (let ((trouble (filter-choices (key keys)
		     (ambiguous? (get index key)))))
      (if (fail? trouble)
	  (fail)
	  (begin (logwarn |RegistryError| 
		   (choice-size trouble) " of the " (choice-size keys)
		   " in " registry " have ambiguous references")
	    trouble)))))

(define (registry/repair! registry relns (slotid #f))
  (let* ((index (registry-index registry))
	 (slot (or slotid (registry-slotid registry)))
	 (keys (pick (getkeys index) slot)))
    (debug%watch "REGISTRY/REPAIR!" 
      registry index slot "NKEYS" (choice-size keys))
    (prefetch-keys! index slot)
    (let ((trouble 
	   (filter-choices (key keys)
	     (ambiguous? (get index key)))))
      (debug%watch "REGISTRY/REPAIR!" "TROUBLE" (choice-size trouble))
      (do-choices (key trouble)
	(let* ((values (get index key))
	       (keep (smallest values))
	       (discard (difference values keep)))
	  (logwarn |FixingRegistry| "Merging into " keep " from " discard)
	  (drop! index key discard)
	  (prefetch-oids! values)
	  (do-choices (discard discard)
	    (logwarn |FixingRegistry/Discard|
	      "Erasing " discard " from existence")
	    (store! discard '%id '(DISCARDED ,(get discarded)))
	    (do-choices (reln relns)
	      (let* ((bg (getopt reln 'index))
		     (findslot (getopt reln 'slotid))
		     (getslot (getopt reln 'adjslot))
		     (fix (find-frames bg findslot discard)))
		;;(%watch "DISCARD" bg findslot getslot "TOFIX" (choice-size fix))
		(prefetch-oids! fix)
		(lock-oids! fix)
		(add! fix slot keep)
		(drop! fix slot discard)
		(drop! bg (cons findslot discard))
		(when getslot
		  (add! fix getslot keep)
		  (drop! fix getslot discard))))))))))





