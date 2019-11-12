#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(config! 'bricosource "brico")

(use-module '{logger webtools varconfig libarchive texttools
	      filestream brico stringfmts optimize
	      reflection})
(use-module '{knodb knodb/branches knodb/typeindex 
	      knodb/flexindex})
(use-module 'brico/build/wikidata)

(config! 'cachelevel 2)
(config! 'logthreadinfo #t)
(config! 'logelapsed #t)
(config! 'thread:logexit #f)
(config! 'dbloglevel %warn%)
(config! 'xprofiling #t)

(define inbufsize (* 1024 1024 3))
(varconfig! inbufsize inbufsize)

(define %loglevel %notice%)

;;; Reading data

(define (skip-file-start port)
  "This skips the opening [ character."
  (getline port))

(define filestream-opts
  [startfn skip-file-start
   readfn getline])

(define (checkpoint in)
  (wikidata/save!)
  (filestream/save! in))

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
	((string? v) (try (get wikidata.props v) v))
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
		     (slot (try (get wikidata.props key) key)))
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
    (checkpoint in)
    (clearcaches)
    (filestream/log! in '(overall))))

(define (main (file "latest-all.json") 
	      (secs (config 'cycletime 120))
	      (cycles (config 'cycles 10))
	      (threadcount (config 'nthreads #t)))
  (unless (config 'wikidata) (config! 'wikidata (abspath "wikidata")))
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
    (checkpoint in)
    (unless (or (file-exists? "read-wikidata.stop") (filestream/done? in))
      (chain file secs cycles threadcount))))
  
(when (config 'optimized #t)
  (optimize! '{knodb knodb/flexpool knodb/adjuncts 
	       knodb/branches knodb/typeindex brico brico/indexing
	       filestream})
  (logwarn |Optimized| 
    "Modules " '{knodb knodb/flexpool knodb/adjuncts 
		 knodb/branches knodb/typeindex brico brico/indexing
		 filestream})
  (optimize!)
  (logwarn |Optimized| (get-source)))

(config! 'profiled filestream/read)

(when (config 'profiled #f)
  (config! 'profiled {get-wikidref new-wikidref import-wikid-item 
		      convert-claims convert-lexslot
		      convert-sitelinks
		      filestream/read}))
