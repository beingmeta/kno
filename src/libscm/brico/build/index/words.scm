#!/usr/bin/knox
;;; -*- Mode: Scheme; -*-

(load-component "common.scm")

(define english @1/2c1c7)

(define lexslots '{%words %norms %glosses %glosses %indicators})

(defambda (index-words f (batch-state #f))
  (prefetch-oids! f)
  (let* ((loop-state (get batch-state 'loop))
	 (core.index (getopt loop-state 'core.index))
	 (words.index (getopt loop-state 'words.index))
	 (frags.index (getopt loop-state 'frags.index))
	 (norms.index  (getopt loop-state 'norms.index))
	 (aliases.index  (getopt loop-state 'aliases.index))
	 (indicators.index  (getopt loop-state 'indicators.index))
	 (glosses.index  (getopt loop-state 'glosses.index))
	 (names.index  (getopt loop-state 'names.index)))
    (do-choices f
      (unless (or (not (test f 'type))
		  (test f 'source @1/1))
	(let* ((%words (get f '%words))
	       (%norms (get f '%norms))
	       (%aliases (get f '%aliases))
	       (%glosses (get f '%glosses))
	       (%indicators (get f '%indicators))
	       (langids (getkeys {%words %norms %glosses %indicators}))
	       (names (pick {words aliases} capitalized?)))
	  (when (exists? %words) (index-frame core.index f 'has '%words))
	  (when (exists? %norms) (index-frame core.index f 'has '%norms))
	  (when (exists? %aliases) (index-frame core.index f 'has '%aliases))
	  (when (exists? %glosses) (index-frame core.index f 'has '%glosses))
	  (when (exists? %indicators) (index-frame core.index f 'has '%indicators))
	  (when (exists? names) (index-frame names.index f 'names {names (downcase names)}))
	  (do-choices (langid (difference langids 'en))
	    (when (test %words langid)
	      (let* ((words (get %words langid))
		     (phrases (pick words compound-string?) ))
		(index-string words.index f (get language-map langid) words)
		(index-frame core.index f 'has (get language-map langid))
		(index-frags frags.index f (get frag-map langid) phrases 2 #t)
		(when (exists? phrases)
		  (index-frame core.index f 'has (get frag-map langid)))))
	    (when (test %norms langid)
	      (index-string norms.index f (get norm-map langid)
			    (get %norms langid))
	      (index-frame core.index 'has (get norm-map langid)))
	    (when (test %aliases langid)
	      (index-string aliases.index f (get alias-map langid)
			    (get %aliases langid))
	      (index-frame core.index 'has (get alias-map langid)))
	    (when (test %indicators langid)
	      (index-string indicators.index f (get indicator-map langid)
			    (get %indicators langid))
	      (index-frame core.index 'has (get gloss-map langid)))
	    (when (test %glosses langid)
	      (index-gloss glosses.index f (get gloss-map langid)
			   (get %glosses langid))
	      (index-frame core.index f 'has (get gloss-map langid))))))
      (swapout f))))

(define (main . names)
  (config! 'appid "indexwords")
  (when (config 'optimize #t)
    (optimize! '{engine brico brico/indexing brico/lookup}))
  (dbctl core.index 'readonly #f)
  (let* ((pools (use-pool (try (elts names) brico-pool-names)))
	 (core.index (target-index "core.index"))
	 (words.index (target-index "words.index"))
	 (frags.index (target-index "frags.index"))
	 (indicators.index (target-index "indicators.index"))
	 (glosses.index (target-index "glosses.index"))
	 (norms.index (target-index "norms.index"))
	 (aliases.index (target-index "aliases.index"))
	 (names.index (target-index "names.index"))
	 (oids (difference (pool-elts pools) (?? 'source @1/1))))
    (drop! core.index (cons 'has lexslots))
    (engine/run index-words oids
      `#[loop #[core.index ,core.index
		words.index ,words.index 
		frags.index ,frags.index
		indicators.index ,indicators.index
		glosses.index ,glosses.index
		aliases.index ,aliases.index
		norms.index ,norms.index
		names.index ,names.index]
	 counters {words names}
	 logcounters #(words names)
	 batchsize ,(config 'batchsize 5000)
	 logfreq ,(config 'logfreq 50)
	 checkfreq 15
	 checktests ,(engine/delta 'items 100000)
	 checkpoint ,{pools core.index words.index frags.index 
		      indicators.index aliases.index
		      norms.index glosses.index
		      names.index}
	 logfns {,engine/log ,engine/logrusage}
	 logchecks #t])))

(when (config 'optimize #t config:boolean)
  (optimize! '{brico engine fifo brico/indexing})
  (optimize!))
