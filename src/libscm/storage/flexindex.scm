;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2018 beingmeta, inc.  All rights reserved.

(in-module 'storage/flexindex)

(use-module '{ezrecords stringfmts logger texttools})
(use-module '{storage/adjuncts storage/filenames})
(use-module '{storage/flex})

(module-export! '{flex/open-index flex/index})

(define-init %loglevel %notice%)

(define-init flex-indexes (make-hashtable))

(define flex-suffix
  #("." (label serial (isdigit+) #t) "index" (eos)))

(define (get-serial arg)
  (if (string? arg)
      (try (get (text->frames flex-suffix arg) 'serial) #f)
      (if (index? arg)
	  (get-serial (index-source arg))
	  (irritant arg |NotAnIndex| get-serial/flexindex/storage))))
(define (index-file-size index) (file-size (index-source index)))
(define (get-keyslot index) (indexctl index 'keyslot))
(define (get-readonly index) (indexctl index 'readonly))

(define (flex/open-index spec (opts #f))
  (let* ((prefix (textsubst spec (qc ".flexindex" flex-suffix) ""))
	 (fullpath (abspath prefix))
	 (full-prefix (strip-suffix fullpath prefix))
	 (directory (dirname fullpath))
	 (files (pick (pick (getfiles directory)
			    has-prefix fullpath)
		      has-suffix ".index"))
	 (serials (get-serial files))
	 (refpath (textsubst (realpath files) flex-suffix ""))
	 (metadata (frame-create #f))
	 (partition-opts
	  `(,(frame-create #f
	       'register (getopt opts 'register #t)
	       'metadata metadata
	       'background #f)
	    . ,opts)))
    (try (get flex-indexes refpath)
	 (let* ((indexes (open-index (strip-prefix files full-prefix) partition-opts))
		(keyslots {(get-keyslot indexes) (getopt partition-opts 'keyslot {})})
		(writable (reject indexes get-readonly))
		(keyslot (if (fail? keyslots)
			     (getopt partition-opts 'keyslot)
			     (singleton keyslots))))
	   (when (and (fail? indexes) (not (getopt opts 'create)))
	     (irritant fullpath |NoMatchingFiles| flex/open-index))
	   (when (ambiguous? keyslots)
	     (irritant spec |InconsistentKeySlots| flex/open-index
		       (when (getopt opts 'keyslot)
			 (printout "\n  " (getopt opts 'keyslot) "\tREQUESTED"))
		       (do-choices (slot keyslots)
			 (printout "\n  " (or slot "none")
			   (do-choices (match (pick indexes get-keyslot slot) i)
			     (printout (if (> i 0) "\n")
			       "\t" (index-source match)))))))
	     (when (and (exists? keyslot) keyslot)
	       (store! metadata 'keyslot keyslot))
	     (let* ((front (tryif (not (getopt opts 'readonly))
			     (open-index (getopt opts 'front {}) opts)
			     (pick-front writable opts)
			     (make-front fullpath
					 (if (fail? serials) 0 (1+ (largest serials)))
					 (try (largest indexes get-serial) #f)
					 opts)))
		    (aggregate (make-aggregate-index (choice indexes front))))
	       (when keyslot (indexctl aggregate 'keyslot keyslot))
	       (if (and (exists? front) front)
		   (indexctl aggregate 'props 'front front)
		   (indexctl aggregate 'readonly #t))
	       (store! flex-indexes
		       (glom {fullpath (realpath fullpath)} {".flexindex" ""})
		       aggregate)
	       aggregate)))))

(defambda (pick-front indexes opts)
  (let ((maxsize (getopt opts 'maxsize))
	(maxkeys (getopt opts 'maxkeys))
	(candidates {}))
    (do-choices (index indexes)
      (let ((ok #t))
	(when (and ok maxsize (> (index-file-size index) maxsize))
	  (set! ok #f))
	(when (and ok (indexctl index 'metadata 'maxsize)
		   (> (index-file-size index)
		      (indexctl index 'metadata 'maxsize)))
	  (set! ok #f))
	(when (and ok maxkeys
		   (> (indexctl index 'keycount) maxkeys))
	  (set! ok #f))
	(when (and ok (indexctl index 'metadata 'maxkeys)
		   (> (index-file-size index)
		      (indexctl index 'metadata 'maxkeys)))
	  (set! ok #f))
	(when ok (set+! candidates index))))
    (pick-one (largest candidates index-file-size))))

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
	"Created new flexindex partition " index " "
	"for flexindex at " fullpath))
    (tryif index index)))

(define (get-make-opts path model opts)
  (let* ((type (getopt opts 'type (and model (indexctl model 'metadata 'type))))
	 (capacity (getopt opts 'capacity
			   (and model (indexctl model 'capacity))))
	 (make-opts (frame-create #f 'type type 'capacity capacity))
	 (maxsize (getopt opts 'maxsize
			  (and model (indexctl model 'metadata 'maxsize))))
	 (maxkeys (getopt opts 'maxkeys
			  (and model (indexctl model 'metadata 'maxkeys))))
	 (new-opts (cons make-opts opts)))
    (unless type
      (logwarn |MissingIndexType|
	"Can't determine type for new index " (write path) ", "
	"using " 'hashindex)
      (store! make-opts 'type 'hashindex))
    (unless capacity
      (logwarn |MissingIndexCapacity|
	"Can't determine capacity for new index " (write path) ", "
	"using " ($num 100000))
      (store! make-opts 'capacity 100000))
    (when (or maxsize maxkeys)
      (unless (getopt new-opts 'metadata) (store! make-opts 'metadata #[]))
      (when maxsize (store! (getopt new-opts 'metadata) 'maxsize maxsize))
      (when maxkeys (store! (getopt new-opts 'metadata) 'maxsize maxsize)))
    new-opts))
