;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/flexindex)

(use-module '{binio texttools})
(use-module '{ezrecords text/stringfmts kno/statefiles logger varconfig})
(use-module '{knodb/adjuncts knodb/filenames})
(use-module '{knodb})

(module-export! '{flex/open-index flexindex/ref 
		  flexindex/relocate!
		  flexindex? flexindex/front})

(define-init %loglevel %warn%)

(define-init flex-indexes (make-hashtable))

(define default-partition-size (* 3 #mib))
(define default-partition-type 'kindex)
(varconfig! flexindex:minsize default-partition-size config:bytes)
(varconfig! flexindex:type default-partition-type)

(define (flexindex? arg)
  (and (aggregate-index? arg) (exists? (indexctl arg 'props 'flexdef))))

(define flex-suffix
  #("." (label serial (isdigit+) #t) ".index" (eos)))

(define (opt-max prop index opts (optval))
  (default! optval (getopt opts prop #f))
  (let ((index-value (try (indexctl index 'metadata prop) #f)))
    (if (and index-value optval)
	(min index-value optval)
	(or index-value optval))))

(define (index/atmax? index (opts #f))
  (let* ((filesize (index-file-size index))
	 (keycount (indexctl index 'livecount))
	 (buckets (indexctl index 'capacity))
	 (maxsize (opt-max 'maxsize index opts))
	 (maxkeys (opt-max 'maxkeys index opts))
	 (maxload (or (opt-max 'maxload index opts) 1.0))
	 (reserved (try (indexctl index 'props 'reserved) 0))
	 (margin (getopt opts 'margin 0))
	 (load (/~ (+ keycount reserved margin) buckets)))
    (or (and maxsize (> filesize maxsize))
	(and maxkeys (> keycount maxkeys))
	(and maxload (> load maxload)))))

(define (get-serial arg)
  (if (string? arg)
      (get (text->frames flex-suffix arg) 'serial)
      (if (index? arg)
	  (get-serial (index-source arg))
	  (irritant arg |NotAnIndex| get-serial/flexindex/storage))))
(define (index-file-size index) (file-size (index-source index)))
(define (get-keyslot index) (indexctl index 'keyslot))
(define (get-readonly index) (indexctl index 'readonly))

;;; Reading flexindex data

(define (read-flexindex spec (opts #f))
  (when (and (file-exists? spec) (not (file-regular? spec)))
    (if (has-suffix spec ".flexindex")
	(irritant spec |InvalidFile|)
	(set! spec (glom spec ".flexindex"))))
  (cond ((file-regular? spec)
	 (let* ((state (statefile/read spec)))
	   (cond ((not (testopt opts 'keyslot)))
		 ((identical? (getopt opts 'keyslot {}) (get state 'keyslot)))
		 ((and (fail? (get state 'keyslot)) (ambiguous? (getopt opts 'keyslot {})))
		  (logwarn |NoKeySlot|
		    "The flexindex " spec " doesn't specify any keyslots, but our caller "
		    "requested " (getopt opts 'keyslot {})))
		 (else (irritant spec
			   |InconsistentKeyslots| flex/open-index
			   (stringout "Requested " (getopt opts 'keyslot {})
			     ", declared " (get state 'keyslot)))))
	   (get-flex-opts spec opts state)))
	((testopt opts 'create)
	 (let* ((metadata (frame-create (getopt opts 'metadata #[])
			    'partindex  (getopt opts 'partindex (getopt opts 'indextype 'kindex))
			    'maxsize (getopt opts 'maxsize {})
			    'maxkeys (getopt opts 'maxkeys {})
			    'maxload (getopt opts 'maxload (tryif (not (testopt opts 'maxkeys)) 0.5))
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
	   (get-flex-opts spec opts state)))
	(else (irritant spec |NoSuchIndex|))))

(define (get-flex-opts spec opts def)
  (let* ((combined (if opts (cons opts def) def))
	 (prefix (or (getopt combined 'prefix #f)
		     (basename (textsubst spec (qc ".flexindex" ".index" flex-suffix) ""))))
	 (directory (dirname (abspath spec)))
	 (partspec (getopt combined 'partdir #f))
	 (partdir (knodb/checkpath (mkpath (if partspec
					       (mkpath directory partspec)
					       directory)
					   "")
				   opts))
	 (partition-prefix (mkpath partdir prefix))
	 (partopts (frame-create #f
		     'keyslot (getopt combined 'keyslot {})
		     'register (getopt opts 'register #t)
		     'type (getopt combined 'partindex 'kindex)
		     'size (getopt combined 'partsize default-partition-size)
		     'maxkeys (getopt combined 'maxkeys {})
		     'maxsize (getopt combined 'maxsize {})
		     'maxload (getopt combined 'maxload {})
		     'background #f))
	 (computed (frame-create #f
		     'flexdef def 'openopts opts
		     'prefix prefix directory directory
		     'partdir partdir
		     'partition-prefix partition-prefix
		     'partopts (cons partopts (getopt opts 'partopts opts)))))
    (cons computed combined)))

;;; Opening flexindexes

(define (flex/open-index spec (opts #f))
  (let* ((flex (read-flexindex spec opts))
	 (partition-prefix (getopt flex 'partition-prefix))
	 (files (pick (getfiles (getopt flex 'partdir))
		      string-matches? `#(,partition-prefix "." (isdigit+) ".index")))
	 ;; (files (pick (pick (getfiles (getopt flex 'partdir)) has-prefix (glom partition-prefix ".")) has-suffix ".index"))
	 (include (getopt opts 'include {}))
	 (serials (get-serial files))
	 (metadata (getopt flex 'metadata))
	 (partition-opts (getopt flex 'partopts))
	 (aggregate-opts
	  `(,(frame-create #f
	       'label (getopt flex 'label spec)
	       'source (if (file-exists? spec) spec {})
	       'canonical (if (file-exists? spec) (realpath spec) {})
	       'keyslot (getopt flex 'keyslot)
	       'register (getopt opts 'register #t)
	       'background #f)
	    . ,flex)))
    (try (tryif (getopt opts 'justfront)
	   (get flex-indexes `(FRONT ,partition-prefix))
	   (get flex-indexes `(FRONT ,(realpath partition-prefix))))
	 (get flex-indexes partition-prefix)
	 (get flex-indexes (realpath partition-prefix))
	 (let* ((indexes (if (getopt opts 'justfront)
			     (open-index (find-front files partition-opts) partition-opts)
			     (open-index files partition-opts)))
		(keyslots {(get-keyslot indexes) (getopt partition-opts 'keyslot {})})
		(writable (reject indexes get-readonly))
		(keyslot (if (fail? keyslots)
			     (getopt partition-opts 'keyslot)
			     (singleton keyslots)))
		(partition-opts (getopt flex 'partopts)))
	   (when (and (fail? indexes) (not (getopt opts 'create))
		      (not (getopt opts 'readonly)))
	     (irritant partition-prefix |NoMatchingFiles| flex/open-index))
	   (when (and (exists? keyslot) keyslot)
	     (store! metadata 'keyslot keyslot))
	   (let* ((front (tryif (not (getopt opts 'readonly))
			   (open-index (getopt opts 'front {}) partition-opts)
			   (pick-front writable partition-opts)
			   (make-front (getopt flex 'partition-prefix)
				       (if (fail? serials) 0 (1+ (largest serials)))
				       (try (largest indexes get-serial) #f)
				       partition-opts
				       spec)))
		  (one-result (and (singleton? (choice indexes front include)) (getopt opts 'readonly)))
		  (aggregate
		   (if one-result
		       (choice indexes front include)
		       (make-aggregate-index (choice indexes front include) aggregate-opts))))
	     (unless one-result
	       (indexctl aggregate 'props 'flexdef (getopt flex 'flexdef))
	       (indexctl aggregate 'props 'flexopts flex)
	       (indexctl aggregate 'props 'flexspec spec)
	       (indexctl aggregate 'props 'flexlock (make-mutex)))
	     (lognotice |Flexindex| 
	       "Opened flexindex " spec
	       " with " ($count (|| indexes) "partitions")
	       (if (exists? front)
		   (printout " and \nfront " front)
		   " and no front (readonly)"))
	     (unless one-result
	       (open-partition aggregate (dbctl aggregate 'partitions)))
	     (when keyslot (indexctl aggregate 'keyslot keyslot))
	     (if (and (exists? front) front)
		 (begin (indexctl aggregate 'props 'front front)
		   (indexctl aggregate 'readonly #f))
		 (indexctl aggregate 'readonly #t))
	     (store! flex-indexes
		 (if (getopt opts 'justfront)
		     (cons 'FRONT (glom {partition-prefix (realpath partition-prefix)} {".flexindex" ""}))
		     (glom {partition-prefix (realpath partition-prefix)} {".flexindex" ""}))
	       aggregate)
	     aggregate)))))

(define (flexindex/ref filename (opts #f))
  (unless (has-prefix filename "/")
    (set! filename (abspath filename)))
  (try (tryif (and (file-exists? filename) (not (testopt opts 'shared #f)))
	 (or (source->index filename) {}))
       (flex/open-index filename opts)))

(define (flexindex/getfiles directory flex-opts)
  (let* ((prefixspec (getopt flex-opts 'prefix))
	 (prefix (getopt flex-opts 'prefix "part"))
	 (partspec (getopt flex-opts 'partdir))
	 (partdir (knodb/checkpath (mkpath (if partspec
					       (mkpath directory partspec)
					       directory)
					   "")
				   flex-opts))
	 (full-prefix (mkpath partdir prefix)))
    (pick (getfiles partdir)
      string-matches? `#(,full-prefix "." (isdigit+) ".index"))))

(define (open-partition flexindex spec (opts #f))
  (local source (if (index? flexindex) (indexctl flexindex 'source) flexindex))
  (if (index? spec)
      (begin 
	(dbctl spec 'props 'flexindex source)
	spec)
      (let ((index (open-index spec opts)))
	(dbctl index 'props 'flexindex source)
	index)))

(define (extract-serial filename)
  (string->number (slice (gather #("." (isdigit+) ".index") filename)
			 1 -6)))

(define (front-sortfn ix)
  (local source (indexctl ix 'source))
  (vector (file-size source) (extract-serial source)))
(define (front-file-sortfn source)
  (vector (file-size source) (extract-serial source)))

(defambda (pick-front indexes opts)
  (pick-one (smallest (filter-choices (index indexes)
			(not (index/atmax? index opts)))
		      front-sortfn)))

(defambda (find-front files opts)
  (let ((maxsize (getopt opts 'maxsize))
	(maxload (getopt opts 'maxload))
	(maxkeys (getopt opts 'maxkeys))
	(margin (getopt opts 'margin 0))
	(candidates {})
	(front #f))
    (doseq (file (rsorted files front-file-sortfn))
      (when (and (not front) (file-writable? file))
	(let* ((index (open-index file (cons [register #f] opts)))
	       (filesize (index-file-size index))
	       (keycount (try (indexctl index 'livecount) 0))
	       (buckets (indexctl index 'capacity))
	       (maxsize (opt-max 'maxsize index opts))
	       (maxkeys (opt-max 'maxkeys index opts))
	       (maxload (or (opt-max 'maxload index opts) 1.0))
	       (load (/~ (+ keycount margin) buckets)))
	  (unless (or (and maxsize (> filesize maxsize))
		      (and maxkeys (> keycount maxkeys))
		      (and maxload (> load maxload)))
	    (set! front file)))))
    (tryif front front)))

(define (make-front fullpath serial model opts parent)
  (let* ((path (mkpath (dirname fullpath)
		       (glom (basename fullpath) "." (padnum serial 3) ".index")))
	 (make-opts (get-make-opts fullpath model opts))
	 (index (onerror (make-index path make-opts) #f)))
    (unless index
      (logwarn |FAILED/GrowFlexIndex|
	"Couldn't create a new flexindex partition " (write path)))
    (when index
      (when parent (open-partition parent index))
      (logwarn |NewPartition|
	"Created new flexindex partition " (index-source index) " with "
	($count (indexctl index 'capacity) "bucket") " for " parent
	"\n" index))
    (tryif index index)))

(define (get-make-opts path model opts)
  (let* ((type (getopt opts 'type (try (and model (indexctl model 'metadata 'type)) #f)))
	 (buckets (getopt opts 'buckets
			  (try (and model (indexctl model 'capacity)) #f)))
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

;;;; Getting a good (possibly new) front from an existing flexindex

(define (flexindex/front arg (opts #f))
  (cond ((not (index? arg)) (irritant arg |NotAnIndex|))
	((fail? (dbctl arg 'props 'flexdef))
	 (if (dbctl arg 'readonly)
	     (irritant arg |ReadOnlyIndex| "From flexindex/front")
	     arg))
	(else (let ((flexopts (indexctl arg 'props 'flexopts))
		    (current (indexctl arg 'props 'front)))
		(if (and (exists? current) current (not (index/atmax? current flexopts)))
		    current
		    (get-front arg opts))))))
(module-export! 'flexindex/front)

(define (get-front flexindex (opts #f))
  (let ((current (indexctl flexindex 'props 'front))
	(state (indexctl flexindex 'props 'flexopts))
	(directory (dirname (index-source flexindex)))
	(spec (indexctl flexindex 'props 'flexspec))
	(force (getopt opts 'force #f))
	(newfront #f))
    (with-lock (indexctl flexindex 'props 'flexlock)
      (let ((indexes (indexctl flexindex 'partitions)))
	(debug%watch "get-front/entry" 
	  flexindex (indexctl flexindex 'props 'flexlock)
	  indexes current)
	(set! newfront
	  (try (tryif (not force)
		 (pick-front indexes (if opts (cons opts state) state)))
	       (let ((files (flexindex/getfiles directory state)))
		 (try (tryif (not force)
			(let* ((unused-files (difference (realpath files) (realpath (index-source indexes))))
			       (use-file (find-front unused-files (if opts (cons opts state) state)))
			       (front (tryif (and (exists? use-file) use-file)
					(open-index use-file))))
			  front))
		      (let* ((serials (get-serial files))
			     (keyslot (getopt state 'keyslot {}))
			     (partdir (if (getopt state 'partdir)
					  (mkpath directory (getopt state 'partdir))
					  directory))
			     (full-prefix (mkpath partdir (getopt state 'prefix)))
			     (partition-metadata (getopt (getopt state 'flexopts) 'partopts))
			     (new-partition-opts
			      (cons (frame-create #f 
				      'register (getopt opts 'register #t) 'background #f
				      'type (getopt state 'partindex 'kindex)
				      'size (getopt state 'partsize default-partition-size)
				      'maxsize (getopt state 'maxsize {})
				      'maxkeys (getopt state 'maxkeys {})
				      'metadata (tryif partition-metadata partition-metadata)
				      'keyslot keyslot)
				    opts))
			     (front (make-front full-prefix
						(if (fail? serials) 0 (1+ (largest serials)))
						(try (largest indexes get-serial) #f)
						new-partition-opts
						spec)))
			front)))))
	(debug%watch "get-front/exit" 
	  flexindex (indexctl flexindex 'props 'flexlock)
	  newfront)
	(indexctl newfront 'reserved (getopt opts 'margin 0))
	(indexctl flexindex 'props 'readonly #f)
	(indexctl flexindex 'props 'front newfront)))
    (unless (identical? current newfront)
      (lognotice |NewFront|
	"For " (if (testopt opts 'margin) 
		   (printout "a margin of " (getopt opts 'margin) " keys on "))
	flexindex
	"\n  new=" (indexctl newfront 'keycount) "/" (indexctl newfront 'capacity) 
	" =\t" newfront
	"\n  old=" (indexctl current 'keycount) "/" (indexctl current 'capacity)
	" =\t" current))
    newfront))

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

