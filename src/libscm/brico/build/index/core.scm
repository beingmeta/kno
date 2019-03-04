#!/usr/bin/fdexec
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(poolctl brico.pool 'readonly #f)

(use-module 'tinygis)

(define %volatile '{fixup})

(define (prefetcher oids done)
  (when done (commit) (clearcaches))
  (unless done (indexer/prefetch (qc oids))))

(define index-also '{ticker})

(define core-index (target-file "core.index"))
(define wordnet-index (target-file "wordnet.index"))
(define wikid-index (target-file "wikid.index"))
(define latlong-index (target-file "latlong.index"))

(define (index-latlong index f)
  (when (test f '{lat long})
    (index-frame index f 'lat (get-degree-keys (picknums (get f 'lat))))
    (index-frame index f 'long (get-degree-keys (picknums (get f 'long)))))
  (when (test f 'lat/long)
    (index-frame index f 'lat (get-degree-keys (first (get f 'lat/long))))
    (index-frame index f 'long (get-degree-keys (second (get f 'lat/long))))))

(define (reporterror ex) (message "Error in indexcore: " ex))

(define (get-derived-slots f)
  {(get language-map (car (pick (get f '%words) pair?)))
   (get norm-map (car (pick (get f '%norm) pair?)))
   (get indicator-map (car (pick (get f '%signs) pair?)))
   (get gloss-map (car (pick (get f '%gloss) pair?)))
   (get alias-map (car (pick (get f '%aliases) pair?)))
   (get language-map (getkeys (pick (get f '%words) slotmap?)))
   (get norm-map (getkeys (pick (get f '%norm) slotmap?)))
   (get indicator-map (getkeys (pick (get f '%signs) slotmap?)))
   (get gloss-map (getkeys (pick (get f '%gloss) slotmap?)))
   (get alias-map (getkeys (pick (get f '%aliases) slotmap?)))})

(define fixup #f)
(define (indicator-fixup concept)
  (when (test concept '%index)
    (let ((indicators (pick (get concept '%index) pair?)))
      (when (exists? indicators) 
	(add! concept '%indicators indicators)
	(drop! concept '%index indicators)))))

(defambda (indexer frames batch-state loop-state task-state)
  (let ((latlong.table (make-hashtable))
	(wordnet.table (make-hashtable))
	(wikid.table (make-hashtable))
	(core.table (make-hashtable)))
    (prefetch-oids! frames)
    (do-choices (f frames)
      (onerror
	  (begin
	    (when fixup (fixup f))
	    (index-frame core.table f '{type source %linked has})
	    (index-frame core.table f 'has (getslots f))
	    (when (test f 'words) (index-frame core.table f 'has english))
	    (index-frame core.table f 'has (get-derived-slots f))
	    (unless (test f 'source @1/1"Derived from 1911 Roget Thesaurus")
	      (index-frame wordnet.table
		  f '{type words hypernym hyponym sensecat source has
		      sensekeys synsets
		      verb-frames pertainym
		      lex-fileno})
	      (index-frame wordnet.table f '%sensekeys (getvalues (get f '%sensekeys)))
	      (index-frame wordnet.table f 'has (getkeys f))
	      (index-gloss wordnet.table f 'gloss)
	      (index-brico core.table f)
	      (index-frame core.table f index-also)
	      (index-wikid wikid.table f)
	      (index-latlong latlong.table f)))
	  (lambda (ex) (logwarn |IndexError| "Indexing " f "\n" ex))))
    (info%watch "INDEXER"
      "FRAMES" (choice-size frames) latlong.table core.table
      "LATLONG.INDEX" (get loop-state 'latlong.index)
      "CORE.INDEX" (get loop-state 'core.index))
    (index/merge! (get loop-state 'latlong.index) latlong.table)
    (index/merge! (get loop-state 'core.index) core.table)
    (index/merge! (get loop-state 'wikid.index) wikid.table)
    (index/merge! (get loop-state 'wordnet.index) wordnet.table)
    (swapout frames)))

(define (main . names)
  (config! 'appid  "indexcore ")
  (let* ((pools (use-pool (if (null? names) brico.pool
			      (elts names))))
	 (core.index (target-index core-index))
	 (latlong.index (target-index latlong-index))
	 (wordnet.index (target-index wordnet-index))
	 (wikid.index (target-index wikid-index)))
    (engine/run indexer (pool-elts pools)
      `#[loop #[latlong.index ,latlong.index
		core.index ,core.index
		wordnet.index ,wordnet.index
		wikid.index ,wikid.index]
	 batchsize 25000 batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools latlong.index core.index wordnet.index}
	 logfns {,engine/log ,engine/logrusage}
	 logfreq ,(config 'logfreq 50)
	 logchecks #t])
    (swapout)
    (let ((wordforms (find-frames core.index 'type 'wordform)))
      (lognotice |Wordforms|
	"Indexing " ($count (choice-size wordforms) "wordform"))
      (prefetch-oids! wordforms)
      (do-choices (f wordforms)
	(index-frame wordnet.index f '{word of sensenum language rank type}))
      (commit wordnet.index))))

(when (config 'optimize #t)
  (optimize! '{brico brico/indexing tinygis fifo engine})
  (optimize!))

