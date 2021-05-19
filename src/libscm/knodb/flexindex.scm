;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2021 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/flexindex)

(use-module '{binio texttools})
(use-module '{ezrecords text/stringfmts kno/statefiles logger varconfig})
(use-module '{knodb/adjuncts knodb/filenames})
(use-module '{knodb})

(module-export! '{flex/open-index flexindex/relocate!})

(define-init %loglevel %notice%)

(define-init flex-indexes (make-hashtable))

(define default-partition-size (* 3 #mib))
(define default-partition-type 'kindex)
(varconfig! flexindex:minsize default-partition-size config:bytes)
(varconfig! flexindex:type default-partition-type)

(define flex-suffix
  #("." (label serial (isdigit+) #t) ".index" (eos)))

(define (get-serial arg)
  (if (string? arg)
      (get (text->frames flex-suffix arg) 'serial)
      (if (index? arg)
	  (get-serial (index-source arg))
	  (irritant arg |NotAnIndex| get-serial/flexindex/storage))))
(define (index-file-size index) (file-size (index-source index)))
(define (get-keyslot index) (indexctl index 'keyslot))
(define (get-readonly index) (indexctl index 'readonly))

(define (get-flex-opts spec (opts #f))
  (when (and (file-exists? spec) (not (file-regular? spec)))
    (if (has-suffix spec ".flexindex")
	(irritant spec |InvalidFile|)
	(set! spec (glom spec ".flexindex"))))
  (cond ((file-regular? spec)
	 (let* ((flex-opts (statefile/read spec)))
	   (cond ((not (testopt opts 'keyslot)))
		 ((identical? (getopt opts 'keyslot {}) (get flex-opts 'keyslot)))
		 ((and (fail? (get flex-opts 'keyslot)) (ambiguous? (getopt opts 'keyslot {})))
		  (logwarn |NoKeySlot|
		    "The flexindex " spec " doesn't specify any keyslots, but our caller "
		    "requested " (getopt opts 'keyslot {})))
		 (else (irritant spec
			   |InconsistentKeyslots| flex/open-index
			   (stringout "Requested " (getopt opts 'keyslot {})
			     ", declared " (get flex-opts 'keyslot)))))
	   (if opts (cons opts flex-opts) flex-opts)))
	((testopt opts 'create)
	 (let* ((metadata (frame-create (getopt opts 'metadata #[])
			    'partindex  (getopt opts 'partindex (getopt opts 'indextype 'kindex))
			    'maxsize (getopt opts 'maxsize {})
			    'maxkeys (getopt opts 'maxkeys {})
			    'maxload (getopt opts 'maxload (tryif (not (testopt opts 'maxkeys)) 2))
			    'partsize (getopt opts 'partsize #2mib)))
		(state (frame-create #f
			 'defpath spec
			 'defabspath (abspath spec)
			 'partdir (getopt opts 'partdir {})
			 'prefix (getopt opts 'prefix (basename spec #t))
			 'label (getopt opts 'label {})
			 'partindex  (getopt opts 'partindex (getopt opts 'indextype 'kindex))
			 'created (timestamp)
			 'maxsize (get metadata 'maxsize)
			 'maxkeys (get metadata 'maxkeys)
			 'maxload (get metadata 'maxload)
			 'keyslot (getopt opts 'keyslot {})
			 'partsize (get opts 'partsize)
			 'metadata metadata)))
	   (statefile/save! state [useformat 'xtype] spec)
	   (if opts (cons opts state) state)))
	(else (irritant spec |NoSuchIndex|))))

(define (flex/open-index spec (opts #f))
  (let* ((flex-opts (get-flex-opts spec opts))
	 (prefixspec (try (getopt opts 'prefix (get flex-opts 'prefix)) #f))
	 (prefix (or prefixspec
		     (basename (textsubst spec (qc ".flexindex" ".index" flex-suffix) ""))))
	 (directory (dirname (abspath spec)))
	 (partspec (try (getopt opts 'partdir (get flex-opts 'partdir)) #f))
	 (partdir (knodb/checkpath (mkpath (if partspec
					       (mkpath directory partspec)
					       directory)
					   "")
				   opts))
	 (full-prefix (mkpath partdir prefix))
	 (files (pick (pick (getfiles partdir)
			has-prefix (glom full-prefix "."))
		  has-suffix ".index"))
	 (include (getopt opts 'include {}))
	 (serials (get-serial files))
	 (refpath (textsubst files flex-suffix ""))
	 (metadata (getopt flex-opts 'metadata))
	 (partition-opts
	  `(,(frame-create #f
	       'keyslot (getopt flex-opts 'keyslot)
	       'register (getopt opts 'register #t)
	       'background #f)
	    . ,opts))
	 (aggregate-opts
	  `(,(frame-create #f
	       'label (getopt flex-opts 'label spec)
	       'source (if (file-exists? spec) spec {})
	       'canonical (if (file-exists? spec) (realpath spec) {})
	       'keyslot (getopt flex-opts 'keyslot)
	       'register (getopt opts 'register #t)
	       'background #f)
	    . ,flex-opts)))
    (try (tryif (getopt opts 'justfront)
	   (get flex-indexes `(FRONT ,refpath)))
	 (get flex-indexes refpath)
	 (let* ((indexes (if (getopt opts 'justfront)
			     (open-index (find-front files partition-opts) partition-opts)
			     (open-index files partition-opts)))
		(keyslots {(get-keyslot indexes) (getopt partition-opts 'keyslot {})})
		(writable (reject indexes get-readonly))
		(keyslot (if (fail? keyslots)
			     (getopt partition-opts 'keyslot)
			     (singleton keyslots)))
		(new-partition-opts
		 (cons (frame-create #f 
			 'register (getopt opts 'register #t) 'background #f
			 'type (getopt flex-opts 'partindex 'kindex)
			 'size (getopt flex-opts 'partsize default-partition-size)
			 'keyslot keyslot)
		       opts)))
	   (when (and (fail? indexes) (not (getopt opts 'create)))
	     (irritant full-prefix |NoMatchingFiles| knodb/open-index))
	   (when (and (exists? keyslot) keyslot)
	     (store! metadata 'keyslot keyslot))
	   (let* ((front (tryif (not (getopt opts 'readonly))
			   (open-index (getopt opts 'front {}) partition-opts)
			   (pick-front writable partition-opts)
			   (make-front full-prefix
				       (if (fail? serials) 0 (1+ (largest serials)))
				       (try (largest indexes get-serial) #f)
				       new-partition-opts
				       spec)))
		  (aggregate
		   (if (and (singleton? (choice indexes front)) (getopt opts 'readonly))
		       (choice indexes front include)
		       (make-aggregate-index (choice indexes front include) aggregate-opts))))
	     (lognotice |Flexindex| 
	       "Opened flexindex " spec
	       " with " ($count (|| indexes) "partition")
	       (if (exists? front)
		   (printout " and \nfront " front)
		   " and no front (readonly)"))
	     (when keyslot (indexctl aggregate 'keyslot keyslot))
	     (if (and (exists? front) front)
		 (indexctl aggregate 'props 'front front)
		 (indexctl aggregate 'readonly #t))
	     (store! flex-indexes
		 (if (getopt opts 'justfront)
		     (cons 'FRONT (glom {full-prefix (realpath full-prefix)} {".flexindex" ""}))
		     (glom {full-prefix (realpath full-prefix)} {".flexindex" ""}))
	       aggregate)
	     aggregate)))))

(define (opt-max index prop opts-value)
  (let ((index-value (try (indexctl index 'metadata prop) #f)))
    (if (and index-value opts-value)
	(min index-value opts-value)
	(or index-value opts-value))))

(defambda (pick-front indexes opts)
  (let ((maxsize (getopt opts 'maxsize #f))
	(maxload (getopt opts 'maxload #f))
	(maxkeys (getopt opts 'maxkeys #f))
	(candidates {}))
    (do-choices (index indexes)
      (let* ((filesize (index-file-size index))
	     (keycount (indexctl index 'keycount))
	     (buckets (indexctl index 'capacity))
	     (maxsize (opt-max index 'maxsize maxsize))
	     (maxkeys (opt-max index 'maxkeys maxkeys))
	     (maxload (or (opt-max index 'maxload maxload) 1.0))
	     (load (/~ (+ keycount (getopt opts 'addkeys 0))
		       buckets)))
	(unless (or (and maxsize (> filesize maxsize))
		    (and maxkeys (> keycount maxkeys))
		    (and maxload (> load maxload)))
	  (set+! candidates index))))
    (pick-one (smallest candidates index-file-size))))

;; (defambda (pick-front indexes opts)
;;   (let ((maxsize (getopt opts 'maxsize))
;; 	(maxload (getopt opts 'maxload))
;; 	(maxkeys (getopt opts 'maxkeys))
;; 	(candidates {}))
;;     (do-choices (index indexes)
;;       (let* ((filesize (index-file-size index))
;; 	     (keycount (indexctl index 'keycount))
;; 	     (buckets (indexctl index 'capacity))
;; 	     (maxsize (opt-max index 'maxsize maxsize))
;; 	     (maxkeys (opt-max index 'maxkeys maxkeys))
;; 	     (maxload (or (opt-max index 'maxload maxload) 1.0))
;; 	     (load (/~ (+ keycount (getopt opts 'addkeys 0))
;; 		       buckets)))
;; 	(unless (or (and maxsize (> filesize maxsize))
;; 		    (and maxkeys (> keycount maxkeys))
;; 		    (and maxload (> load maxload)))
;; 	  (set+! candidates index))))
;;     (pick-one (smallest candidates index-file-size))))

(defambda (find-front files opts)
  (let ((maxsize (getopt opts 'maxsize))
	(maxload (getopt opts 'maxload))
	(maxkeys (getopt opts 'maxkeys))
	(candidates {}))
    (do-choices (file files)
      (when (file-writable? file)
	(let* ((index (open-index file (cons [register #f] opts)))
	       (filesize (index-file-size index))
	       (keycount (indexctl index 'keycount))
	       (buckets (indexctl index 'capacity))
	       (maxsize (opt-max index 'maxsize maxsize))
	       (maxkeys (opt-max index 'maxkeys maxkeys))
	       (maxload (or (opt-max index 'maxload maxload) 1.0))
	       (load (/~ (+ keycount (getopt opts 'addkeys 0))
			 buckets)))
	  (unless (or (and maxsize (> filesize maxsize))
		      (and maxkeys (> keycount maxkeys))
		      (and maxload (> load maxload)))
	    (set+! candidates file)))))
    (pick-one (smallest candidates file-size))))

(define (make-front fullpath serial model opts parent)
  (let* ((path (mkpath (dirname fullpath)
		       (glom (basename fullpath) "." (padnum serial 3) ".index")))
	 (make-opts (get-make-opts fullpath model opts))
	 (index (onerror (make-index path make-opts) #f)))
    (unless index
      (logwarn |FAILED/GrowFlexIndex|
	"Couldn't create a new flexindex partition " (write path)))
    (when index
      (logwarn |NewPartition|
	"Created new flexindex partition " (index-source index) " with "
	($count (dbctl index 'metadata 'buckets) "bucket") " for " parent
	"\n" index))
    (tryif index index)))

(define (get-make-opts path model opts)
  (let* ((type (getopt opts 'type (try (and model (indexctl model 'metadata 'type)) #f)))
	 (buckets (getopt opts 'buckets
			  (try (and model (indexctl model 'metadata 'buckets)) #f)))
	 (size (getopt opts 'size (try (and model (indexctl model 'metadata 'size)) #f)))
	 (make-opts (frame-create #f 'type type))
	 (maxsize (getopt opts 'maxsize
			  (and model (indexctl model 'metadata 'maxsize))))
	 (maxkeys (getopt opts 'maxkeys
			  (and model (indexctl model 'metadata 'maxkeys))))
	 (new-opts (cons make-opts opts)))
    (unless type
      (logwarn |MissingIndexType|
	"Can't determine type for new index " (write path) 
	" from model " model 
	", using " 'kindex)
      (store! make-opts 'type 'kindex))
    
    (if buckets
	(store! make-opts 'buckets buckets)
	(if size
	    (store! make-opts 'size size)
	    (begin (logwarn |MissingIndexSize|
		     "Can't determine capacity for new index " (write path) " from model " model 
		     ", using " ($num 100000))
	      (store! make-opts 'size 100000))))
    
    (when (or (getopt opts 'keyslot) (and model (indexctl model 'keyslot)))
      (store! make-opts 'keyslot 
	(or (getopt opts 'keyslot) (and model (indexctl model 'keyslot)))))
    
    (when (or maxsize maxkeys)
      (unless (getopt new-opts 'metadata) (store! make-opts 'metadata #[]))
      (when maxsize (store! (getopt new-opts 'metadata) 'maxsize maxsize))
      (when maxkeys (store! (getopt new-opts 'metadata) 'maxsize maxsize)))
    new-opts))


;;;; Relocating flexindexes

(define (get-prefix-dir prefix)
  (cond ((has-suffix prefix "/") prefix)
	((file-directory? prefix) prefix)
	((position #\/ prefix) (dirname prefix))
	(else #f)))

(define (strip-partdir prefix partdir)
  (if (not partdir) prefix
      (let ((absprefix (realpath prefix))
	    (abspartdir (mkpath (realpath partdir) "")))
	(if (has-prefix absprefix abspartdir)
	    (slice absprefix (length abspartdir))
	    prefix))))

(define (base-prefix? string prefix) (has-prefix (basename string) prefix))

(define (flexindex/relocate! file new-prefix (newloc #f))
  (let* ((state (statefile/read file))
	 (rootdir (dirname file))
	 (partdir (if (test state 'partdir) 
		      (knodb/abspath (get state 'partdir) (dirname file))
		      (knodb/abspath (dirname file))))
	 (prefix (try (get state 'prefix) #f))
	 (new-root (if newloc (dirname (abspath newloc)) rootdir))
	 (new-partdir (get-prefix-dir (knodb/abspath new-prefix new-root)))
	 (new-prefix (or (strip-partdir (abspath new-prefix new-root) new-partdir) "")))
    (debug%watch "FLEXINDEX/RELOCATE!" partdir prefix new-partdir new-prefix)
    (do-choices (partition (pick (getfiles partdir) base-prefix? prefix))
      (let* ((base (basename partition))
	     (newbase (textsubst base `#((bos) (not> ".")) new-prefix)))
	(if newloc
	    (move-file partition (mkpath (mkpath new-root new-partdir) newbase))
	    (move-file partition (mkpath new-partdir newbase)))))
    (when newloc (store! state 'statefile (abspath newloc)))
    (store! state 'partdir (knodb/relpath new-partdir new-root))
    (store! state 'prefix (knodb/relpath new-prefix new-root))
    (statefile/save! state)
    (when (and newloc (not (equal? (abspath newloc) (abspath file))))
      (move-file file (glom file ".bak")))))

