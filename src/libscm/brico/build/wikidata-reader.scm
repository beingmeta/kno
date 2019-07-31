#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource (get-component "brico"))

(use-module '{logger webtools varconfig libarchive texttools brico stringfmts optimize})
(use-module '{flexdb flexdb/branches flexdb/typeindex})

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'dbloglevel %warn%)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

;; (define wikidata.pool
;;   (flexdb/make (get-component "wikidata/wikdiata.pool")
;; 	       [base @31c1/0 capacity (* 16 1024 1024) 
;; 		type 'bigpool create #t 
;; 		adjuncts #[labels #[pool "labels.pool"]
;; 			   aliases #[pool "aliases.pool"]
;; 			   claims #[pool "claims.pool"]
;; 			   sitelinks #[pool "sitelinks.pool"]]]))
(define wikidata.pool
  (flexdb/make "wikidata/wikidata.flexpool"
	       [create #t type flexpool
		base @31c1/0 capacity (* 128 1024 1024)
		partsize (* 1024 1024) partition-type 'bigpool
		prefix "pools/"
		adjuncts #[labels #[pool "labels.flexpool"]
			   aliases #[pool "aliases.flexpool"]
			   claims #[pool "claims.flexpool"]
			   sitelinks #[pool "sitelinks.flexpool"]]]))

(define buildmap.table
  (flexdb/make (get-component "wikidata/wikids.table") [indextype 'memindex create #t]))
(define wikids.index
  (flexdb/make (get-component "wikidata/wikids.index")
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		keyslot 'id register #t]))
(define words.index
  (flexdb/make (get-component "wikidata/words.index")
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		keyslot 'words register #t]))
(define norms.index
  (flexdb/make (get-component "wikidata/norms.index")
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		keyslot 'norms register #t]))
;; (define has.index
;;   (typeindex/open (get-component "wikidata/has.index/")
;; 		  [indextype 'typeindex create #t keyslot 'has register #t]
;; 		  #t))
(define has.index
  (flexdb/make (get-component "wikidata/hasprops.index")
	       [indextype 'hashindex create #t keyslot 'has register #t]))
(define props.index
  (flexdb/make (get-component "wikidata/props.index")
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		register #t]))

(define wikidata.index
  (make-aggregate-index {words.index norms.index has.index props.index}))

(define line-count-file (get-component "wikidata/linecount"))

(define line-count
  (if (file-exists? line-count-file)
      (read (open-input-file line-count-file))
      0))

(define (wikidata/save!)
  (flexdb/commit! {wikidata.pool wikidata.index 
		   wikids.index buildmap.table
		   has.index})
  (fileout line-count-file (printout line-count)))

(define propmap
  (let ((props (?? 'type 'wikidprop))
	(table (make-hashtable)))
    (prefetch-oids! props)
    (do-choices (prop props)
      (add! table (get prop 'wikid) prop)
      (add! table (downcase (get prop 'wikid)) prop)
      (add! table (string->symbol (downcase (get prop 'wikid))) prop))
    table))

;;; Reading data

(define (open-wikidata path (line line-count))
  (let ((port (if (has-suffix path {".bz2" ".gz" ".xz"})
		  (archive/open path 0)
		  (open-input-file path)))
	(skip line))
    (setbuf! port inbufsize)
    ;; Skip the [
    (getline port)
    ;; Skip things you've read already
    (until (zero? skip) (getline port) (set! skip (-1+ skip)))
    (cons port line)))

(defslambda (read-item port (line))
  (set! line (getline (car port)))
  (when (and line (= (length line) 0)) (set! line #f))
  (when line (set-cdr! port (1+ (cdr port))))
  line)

(define (get-wikidref id)
  (try (get buildmap.table id)
       (let ((ref (new-wikidref id)))
	 (index-frame wikids.index ref 'id id)
	 ref)))

(defslambda (new-wikidref id)
  (try (get buildmap.table id)
       (let ((f (frame-create wikidata.pool 'id id 'created (gmtimestamp))))
	 (store! buildmap.table id f)
	 f)))

(define (convert-lexslot slotval (new (frame-create #f)))
  (do-choices (key (getkeys slotval))
    (let ((v (get slotval key)))
      (if (vector? v)
	  (add! new key (get (elts v) 'value))
	  (add! new key (get v 'value)))))
  new)

(define (convert-sitelinks slotval (new (frame-create #f)))
  (do-choices (key (getkeys slotval))
    (let ((v (get slotval key)))
      (if (vector? v)
	  (doseq (elt v)
	    (add! new key (frame-create #f
			    'title (get v 'title) 'site (get v 'site)
			    'badges (elts (get v 'badges)))))
	  (add! new key (frame-create #f
			  'title (get v 'title) 'site (get v 'site)
			  'badges (elts (get v 'badges)))))))
  new)

(define typeslots '{type datatype rank snaktype type})
(define refslots '{type datatype rank snaktype type})

(define (convert-claims v)
  (cond ((vector? v)
	 (convert-claims (elts v)))
	((string? v) (try (get propmap v) v))
	((not (table? v)) v)
	(else
	 (when (test v 'references) (drop! v 'references))
	 (try
	  (tryif (test v 'datatype "wikibase-item")
	    (get-wikidref (get (get (get v 'datavalue) 'value) 'id)))
	  (tryif (test v 'entity-type "item")
	    (get-wikidref (get v 'id)))
	  (tryif (test v 'datatype "wikibase-entityid")
	    (get-wikidref (get (get v 'value) 'id)))
	  (let ((converted (frame-create #f))
		(keys (getkeys v)))
	    (do-choices (key keys)
	      (let* ((vals (get v key))
		     (slot (try (get propmap key) key)))
		(when (vector? vals) (set! vals (elts vals)))
		(cond ((overlaps? slot typeslots)
		       (add! converted slot (symbolize vals)))
		      (else (add! converted slot (convert-claims vals))))))
	    converted)))))

(define (import-wikid-item item index has.index)
  (let* ((id (get item 'id))
	 (ref (get-wikidref id)))
    (store! ref 'wikitype (string->symbol (get item 'type)))
    (unless (test ref 'lastrevid (get item 'lastrevid))
      (store! ref 'lastrevid (get item 'lastrevid))
      (store! ref 'labels (convert-lexslot (get item 'labels)))
      (store! ref 'aliases (convert-lexslot (get item 'aliases)))
      (store! ref 'descriptions (convert-lexslot (get item 'descriptions)))
      (store! ref 'sitelinks (convert-sitelinks (get item 'sitelinks)))
      (let* ((norms (get (get ref 'labels) 'en))
	     (words {norms (get (get ref 'aliases) 'en)}))
	(store! ref 'norms norms)
	(store! ref 'words words)
	(index-frame index ref 'words {words (stdstring words)})
	(index-frame index ref 'norms {norms (stdstring norms)}))
      (let* ((claims (convert-claims (get item 'claims)))
	     (props (getkeys claims)))
	(store! ref 'claims claims)
	(do-choices (prop props)
	  (let ((vals (get (get claims prop) 'mainsnak)))
	    (add! ref prop {(pickstrings vals) (pickoids vals)
			    (picknums vals)})
	    (index-frame index ref prop (pickoids vals)))))
      (index-frame has.index ref 'has (getkeys ref)))
    ref))

(define (read-loop in (duration 60) (index wikidata.index) (has.index has.index))
  (let ((branch (index/branch index))
	(started (elapsed-time))
	(saved (elapsed-time)))
    (while (< (elapsed-time started) duration)
      (let* ((line (read-item in))
	     (item (and line (jsonparse line 'symbolize))))
	(import-wikid-item item branch has.index)
	(when (zero? (random 500))
	  (branch/commit! branch)
	  (set! saved (elapsed-time)))))
    (branch/commit! branch)))

(define (thread-loop in (duration 60) (threadcount #t) (index wikidata.index))
  (let ((threads {}))
    (dotimes (i (if (number? threadcount)
		    threadcount
		    (rusage 'ncpus)))
      (set+! threads (thread/call read-loop in duration index)))
    (thread/join threads)
    (set! line-count (cdr in))))

(define ($rate count ticks) (inexact->string (/~ count ticks) 1))

(define (runstats (usage (rusage)))
  (stringout "CPU: " (inexact->string (get usage 'cpu%) 2) "%"
    ", load: " (doseq (v (get usage 'loadavg) i)
	       (printout (if (> i 0) " ") (inexact->string v 2)))
    ", resident: " ($bytes (get usage 'memusage))
    ", virtual: " ($bytes (get usage 'vmemusage))))

(define (dobatch (secs 120) (start-count line-count) (start-time (elapsed-time)))
  (thread-loop in secs)
  (lognotice |Progress|
    "Procesed " ($num (- line-count start-count)) " items in "
    (elapsed-time start-time) "s, or " 
    ($rate (- line-count start-count) (elapsed-time start-time))
    " items/sec")
  (lognotice |Saving|
    "Wikidata after " ($num (- line-count start-count)) " items: " (runstats))
  (wikidata/save!)
  (clearcaches)
  (lognotice |Process|
    "Processed " ($num (- line-count init-count)) " items in "
    (secs->string (elapsed-time started)) ", or " 
    ($rate (- line-count init-count) (elapsed-time started))
    " items/sec")
  (lognotice |Overall| "To date, " ($num line-count) " items have been processed."))

(define in (open-wikidata (get-component "latest-all.json.bz2")))
(define started #f)
(define init-count line-count)

(define (main)
  (unless in (set! in (open-wikidata (get-component "latest-all.json.bz2"))))
  (set! started (elapsed-time))
  (lognotice |Start| "Starting to read wikidata at line #" line-count)
  (let ((cycles (config 'cycles 10)))
    (dotimes (i cycles)
      (lognotice |Cycle| "Starting #" (1+ i) " of " cycles ": " (runstats))
      (dobatch))
    (lognotice |Cycle| "Finished #" cycles ", "
	       "processed " ($num (- line-count init-count)) " items in "
	       (secs->string (elapsed-time started)))
    (lognotice |Cycle| ($rate (- line-count init-count) (elapsed-time started))
	       " items/sec; " (runstats))))
  
(when (config 'optimized #t)
  (optimize! '{flexdb flexdb/branches flexdb/typeindex brico brico/indexing})
  (optimize!)
  (logwarn |Optimzed| "Running code optimized"))



