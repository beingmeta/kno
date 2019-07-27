#!/usr/bin/knox
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
(define wikidref-index (target-file "wikidref.index"))
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

(define wn-sources
  {@1/0{WN16} @1/46074"Wordnet 3.0, Copyright 2006 Princeton University" 
   @1/94d47"Wordnet 3.1, Copyright 2011 Princeton University"})

(define index-sources
  '{@1/0{WN16}
    @1/46074"Wordnet 3.0, Copyright 2006 Princeton University" 
    @1/94d47"Wordnet 3.1, Copyright 2011 Princeton University"
    wikidata})

(defambda (indexer frames batch-state loop-state task-state)
  (let ((index (get batch-state 'index))
	(wikid.index (get batch-state 'wikid.index))
	(latlong.index (get batch-state 'latlong.index))
	(wordnet.index (get batch-state 'wordnet.index)))
    (prefetch-oids! frames)
    (do-choices (f frames)
      (onerror
	  (begin
	    (when fixup (fixup f))
	    (when (test f 'source index-sources)
	      (index-frame wordnet.index
		  f '{type source has words hypernym hyponym sensecat
		      sensekeys synsets verb-frames pertainym
		      lex-fileno})
	      (index-frame wordnet.index f '%sensekeys (getvalues (get f '%sensekeys)))
	      (index-frame wordnet.index f 'has (getkeys f))
	      (index-gloss wordnet.index f 'gloss)
	      (index-brico index f)
	      (index-latlong latlong.index f)
	      (index-frame index f index-also)
	      (index-wikid wikid.index f))))
      (lambda (ex) (logwarn |IndexError| "Indexing " f "\n" ex))))
  (swapout frames))

(define (main . names)
  (config! 'appid  "indexcore")
  (let* ((pools (use-pool (try (elts names) brico-pool-names)))
	 (core.index (target-index core-index))
	 (latlong.index (target-index latlong-index #[keyslot {lat long}]))
	 (wordnet.index (target-index wordnet-index))
	 (wikidref.index (target-index wikidref-index #[keyslot wikidref]))
	 (wikid.index (target-index wikid-index))
	 (index (make-aggregate-index {core.index latlong.index wikidref.index}
				      [register #t])))
    (engine/run indexer (pool-elts pools)
      `#[loop #[index ,index
		wordnet.index ,wordnet.index
		wikid.index ,wikid.index]
	 branchindexes {index wordnet.index wikid.index}
	 batchsize 10000 batchrange 4
	 checkfreq 15
	 checktests ,(engine/interval (config 'savefreq 60))
	 checkpoint ,{pools wikid.index wordnet.index index}
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

(when (config 'optimize #t config:boolean)
  (optimize! '{brico brico/indexing tinygis fifo engine})
  (optimize!))

