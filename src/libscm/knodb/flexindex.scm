;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/flexindex)

(use-module '{binio texttools})
(use-module '{ezrecords text/stringfmts logger varconfig})
(use-module '{knodb/adjuncts knodb/filenames})
(use-module '{knodb})

(module-export! '{flex/open-index})

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

(define (get-flex-opts spec (opts #f) (files #f))
  (when (and (file-exists? spec) (not (file-regular? spec)))
    (if (has-suffix spec ".flexindex")
	(irritant spec |InvalidFile|)
	(set! spec (glom spec ".flexindex"))))
  (cond ((file-regular? spec)
	 (if opts (cons opts (read-xtype spec))
	     (read-xtype spec)))
	((or (testopt opts 'create) files)
	 (let* ((metadata (frame-create (getopt opts 'metadata #[])
			    'partindex  (getopt opts 'partindex (getopt opts 'indextype 'kindex))
			    'maxsize (getopt opts 'maxsize {})
			    'maxkeys (getopt opts 'maxkeys {})
			    'maxload (getopt opts 'maxload (tryif (not (testopt opts 'maxkeys)) 2))
			    'partsize (getopt opts 'partsize #2mib)))
		(state (frame-create #f
			 'path spec
			 'label (getopt opts 'label {})
			 'partindex  (getopt opts 'partindex (getopt opts 'indextype 'kindex))
			 'created (timestamp)
			 'maxsize (get metadata 'maxsize)
			 'maxkeys (get metadata 'maxkeys)
			 'maxload (get metadata 'maxload)
			 'partsize (get opts 'partsize)
			 'metadata metadata)))
	   (write-xtype state spec)
	   (if opts (cons opts state) state)))
	(else (irritant spec |NoSuchIndex|))))

(define (flex/open-index spec (opts #f))
  (let* ((prefix (textsubst spec (qc ".flexindex" ".index" flex-suffix) ""))
	 (fullpath (abspath prefix))
	 (full-prefix (strip-suffix fullpath prefix))
	 (directory (dirname fullpath))
	 (files (pick (pick (getfiles directory)
			has-prefix (glom fullpath "."))
		  has-suffix ".index"))
	 (flex-opts (get-flex-opts spec opts (try files #f)))
	 (include (getopt opts 'include {}))
	 (serials (get-serial files))
	 (refpath (textsubst files flex-suffix ""))
	 (metadata (getopt flex-opts 'metadata))
	 (partition-opts
	  `(,(frame-create #f
	       'keyslot (getopt flex-opts 'keyslot)
	       'register (getopt opts 'register #t)
	       'background #f)
	    . ,opts)))
    (try (get flex-indexes refpath)
	 (let* ((indexes (open-index (strip-prefix files full-prefix) partition-opts))
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
	     (irritant fullpath |NoMatchingFiles| knodb/open-index))
	   (when (and (exists? keyslot) keyslot)
	     (store! metadata 'keyslot keyslot))
	   (let* ((front (tryif (not (getopt opts 'readonly))
			   (open-index (getopt opts 'front {}) partition-opts)
			   (pick-front writable partition-opts)
			   (make-front fullpath
				       (if (fail? serials) 0 (1+ (largest serials)))
				       (try (largest indexes get-serial) #f)
				       new-partition-opts)))
		  (aggregate
		   (if (and (singleton? (choice indexes front)) (getopt opts 'readonly))
		       (choice indexes front include)
		       (make-aggregate-index (choice indexes front include) flex-opts))))
	     (when keyslot (indexctl aggregate 'keyslot keyslot))
	     (if (and (exists? front) front)
		 (indexctl aggregate 'props 'front front)
		 (indexctl aggregate 'readonly #t))
	     (store! flex-indexes
		 (glom {fullpath (realpath fullpath)} {".flexindex" ""})
	       aggregate)
	     aggregate)))))

(define (opt-max index prop opts-value)
  (let ((index-value (indexctl index 'metadata prop)))
    (if (and index-value opts-value)
	(min index-value opts-value)
	(or index-value opts-value))))

(defambda (pick-front indexes opts)
  (let ((maxsize (getopt opts 'maxsize))
	(maxload (getopt opts 'maxload))
	(maxkeys (getopt opts 'maxkeys))
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

(define (make-front fullpath serial model opts)
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
	($count (dbctl index 'metadata 'buckets) "bucket") " for " fullpath
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
