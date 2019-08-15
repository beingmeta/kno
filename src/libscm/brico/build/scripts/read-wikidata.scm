#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource "brico")

(use-module '{logger webtools varconfig libarchive texttools
	      filestream brico stringfmts optimize
	      reflection})
(use-module '{flexdb flexdb/branches flexdb/typeindex 
	      flexdb/flexindex})

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'dbloglevel %warn%)
(config! 'xprofiling #t)

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
	       [create #t type 'flexpool
		base @31c1/0 capacity (* 128 1024 1024)
		partsize (* 1024 1024) pooltypek 'bigpool
		prefix "pools/"
		adjuncts #[labels #[pool "labels"]
			   aliases #[pool "aliases"]
			   claims #[pool "claims"]
			   sitelinks #[pool "sitelinks"]]
		reserve 1]))

(define buildmap.table
  (flexdb/make "wikidata/wikids.table" [indextype 'memindex create #t]))

(define wikids.index
  (flex/open-index "wikidata/wikids.flexindex"
		   [indextype 'hashindex size (* 8 1024 1024) create #t
		    keyslot 'id register #t
		    maxkeys (* 4 1024 1024)]))

(define words.index
  (flex/open-index "wikidata/words.flexindex"
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      keyslot 'words register #t
		      maxkeys (* 2 1024 1024)]))
(define norms.index
  (flex/open-index "wikidata/norms.flexindex"
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      keyslot 'norms register #t
		      maxkeys (* 2 1024 1024)]))
#|
(define words.index
  (flexdb/make "wikidata/words.index"
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		keyslot 'words register #t]))
(define norms.index
  (flexdb/make "wikidata/norms.index"
	       [indextype 'hashindex size (* 16 1024 1024) create #t
		keyslot 'norms register #t]))
|#

;; (define has.index
;;   (typeindex/open (get-component "wikidata/has.index/")
;; 		  [indextype 'typeindex create #t keyslot 'has register #t]
;; 		  #t))
(define has.index
  (flexdb/make "wikidata/hasprops.index"
	       [indextype 'hashindex create #t keyslot 'has register #t]))

(define props.index
  (flex/open-index "wikidata/props.index"
		     [indextype 'hashindex size (* 4 1024 1024) create #t
		      register #t maxkeys (* 2 1024 1024)]))

(define wikidata.index
  (make-aggregate-index {words.index norms.index has.index props.index}))

(define (wikidata/save! in)
  (flexdb/commit! {wikidata.pool wikidata.index 
		   wikids.index buildmap.table
		   has.index})
  (filestream/save! in))

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

(define (skip-file-start port)
  "This skips the opening [ character."
  (getline port))

(define filestream-opts
  [startfn skip-file-start
   readfn getline])

;;; Parsing the data

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

;;; Reporting

(define (runstats (usage (rusage)))
  (stringout "CPU: " (inexact->string (get usage 'cpu%) 2) "%"
    ", load: " (doseq (v (get usage 'loadavg) i)
	       (printout (if (> i 0) " ") (inexact->string v 2)))
    ", resident: " ($bytes (get usage 'memusage))
    ", virtual: " ($bytes (get usage 'vmemusage))))

(define (read-loop in (duration 60) (index wikidata.index) (has.index has.index))
  (let ((branch (index/branch index))
	(started (elapsed-time))
	(saved (elapsed-time)))
    (while (< (elapsed-time started) duration)
      (let* ((line (filestream/read in))
	     (item (and (satisfied? line) (string? line)
			(jsonparse line 'symbolize))))
	(debug%watch "READ-LOOP" line item)
	(when item
	  (import-wikid-item item branch has.index)
	  (when  (and (> (elapsed-time saved) 10)
		      (zero? (random 500)))
	    (branch/commit! branch)
	    (set! saved (elapsed-time))))))
    (branch/commit! branch)))

(define (thread-loop in (threadcount #t) (duration 60) 
		     (index wikidata.index) (has.index has.index))
  (let ((threads {}))
    (dotimes (i (if (number? threadcount)
		    threadcount
		    (rusage 'ncpus)))
      (set+! threads (thread/call read-loop in duration index has.index)))
    (thread/join threads)))

(define (dobatch in (threadcount #t) (duration 120) 
		 (index wikidata.index) (has.index has.index))
  (let ((start-count (filestream-itemcount in))
	(start-time (elapsed-time))
	(before (and (reflect/profiled? filestream/read)
		     (profile/getcalls filestream/read))))
    (if threadcount
	(thread-loop in threadcount duration index has.index)
	(let ((started (elapsed-time))
	      (saved (elapsed-time)))
	  (while (< (elapsed-time started) duration)
	    (let* ((line (filestream/read in))
		   (item (and (satisfied? line) (string? line)
			      (jsonparse line 'symbolize))))
	      (when item
		(import-wikid-item item index has.index))))))
    (let ((cur-count (filestream-itemcount in)))
      (lognotice |Saving|
	"wikidata after processing " ($count (- cur-count start-count) "item") " in "
	(secs->string (elapsed-time start-time)) 
	" (" ($rate (- cur-count start-count) (elapsed-time start-time)) " items/sec) -- "
	(runstats)))
    (when before
      (let ((cur (profile/getcalls filestream/read)))
	(lognotice |ReadProfile|
	  "After " (secs->string (elapsed-time start-time)) 
	  " filestream/read took " 
	  (secs->string (- (profile/time cur) (profile/time before)))
	  " (clock), "
	  (secs->string (- (profile/utime cur) (profile/utime before)))
	  " (user)" 
	  (secs->string (- (profile/stime cur) (profile/stime before)))
	  " (system) across "
	  ($count (- (profile/ncalls cur) (profile/ncalls before)) "call")
	  " interrupted by "
	  ($count (- (profile/waits cur) (profile/waits before)) "wait") )))
    (wikidata/save! in)
    (clearcaches)
    (filestream/log! in '(overall))))

(define (main (file "latest-all.json") 
	      (secs (config 'cycletime 120))
	      (cycles (config 'cycles 10))
	      (threadcount (config 'threads #t)))
  (let ((in (filestream/open file filestream-opts))
	(started (elapsed-time)))
    (filestream/log! in)
    (dotimes (i cycles)
      (lognotice |Cycle| "Starting #" (1+ i) " of " cycles ": " (runstats))
      (dobatch in threadcount secs)
      (lognotice |Cycle| "Finished #" (1+ i) "/" cycles ", "
		 "processed " ($num (filestream-itemcount in) "item") " in "
		 (secs->string (elapsed-time started)))
      (filestream/log! in '(overall)))
    (wikidata/save! in)))
  
(when (config 'optimized #t)
  (optimize! '{flexdb flexdb/flexpool flexdb/adjuncts 
	       flexdb/branches flexdb/typeindex brico brico/indexing
	       filestream})
  (logwarn |Optimized| 
    "Modules " '{flexdb flexdb/flexpool flexdb/adjuncts 
		 flexdb/branches flexdb/typeindex brico brico/indexing
		 filestream})
  (optimize!)
  (logwarn |Optimized| (get-source)))

(config! 'profiled filestream/read)

(when (config 'profiled #f)
  (config! 'profiled {get-wikidref new-wikidref import-wikid-item 
		      convert-claims convert-lexslot
		      convert-sitelinks
		      filestream/read}))
