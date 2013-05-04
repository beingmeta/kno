;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

(in-module 'gnosys/refs)

;;; This provides concept resolution from natural language text

(define version "$Id: $")

(use-module '{texttools gnosys gnosys/nlp gnosys/disambiguate})
(use-module '{brico brico/lookup brico/indexing})

(module-export! '{refs/analyze refs/getentries})

;;;; Tracing support

(define tracedetails #f)
(define traceresults #f)
(define tracetime #t)

(define logger comment)
;;(define logger logif)
(define %volatile '{logger traceresults tracedetails tracetime})

;;;; Configurable stuff

(define fastanalyze #f)

;;;; The core analysis function

(defambda (refs/analyze item (persist #f) (fast fastanalyze)
			(content-slotids 'text) (subject-slotids 'title))
  (for-choices item
    (let* ((now (gmtimestamp))
	   (index (if (index? persist) persist #f))
	   (language (try (get item @?gn/language) (get item 'language)
			  default-language))
	   (title-analysis (if (string? item) (nlp/analyze item)
			       (nlp/analyze item subject-slotids index)))
	   (description-analysis (if (string? item) (fail)
				     (nlp/analyze item content-slotids index)))
	   (allnames (get item @?gn/proper-names))
	   (reduced-names (reduce-names allnames))
	   (full-names (pick reduced-names string?))
	   (name-map (pick reduced-names pair?))
	   (title-names (choice (get title-analysis @?gn/proper-names)
				(get name-map (get title-analysis @?gn/proper-names)))))
      (logger tracedetails "NAME-MAP=" name-map)
      (logger tracedetails "PHRASES=" (write (qc (get item @?gn/phrases))))
      (logger tracedetails "FULL-NAMES=" (write (qc full-names)))
      (logger tracedetails "TITLE-NAMES=" (write (qc title-names)))
      (let* ((start-disambig (elapsed-time))
	     (raw-entries
	      (ambentry/given
	       ;; ambentry/given uses resolved entries to disambiguate
	       ;;  unresolved entries
	       (choice (term->ambentry full-names language 3)
		       (term->ambentry (get item @?gn/phrases) language)
		       (term->ambentry
			(get item '{keywords @?gn/keywords})
			language))))
	     (scored-entries
	      (begin (ambentry/apriori-prefetch! raw-entries)
		     (ambentry/scored raw-entries)))
	     (done-wordfetch (elapsed-time)))
	(logger tracedetails
	  "Identified " (choice-size scored-entries) " terms and "
	  (choice-size (ambentry-candidates scored-entries)) " possible meanings "
	  "in " (- done-wordfetch start-disambig) " seconds")
	(unless fastanalyze (disambiguate-prefetch! scored-entries))
	(let* ((done-prefetch (elapsed-time))
	       (entries
		(if fastanalyze
		    (ambentry/given (ambentry/disambig scored-entries 0.6))
		    (ambentry/given (disambiguate scored-entries -0.5))))
	       (done-disambig (elapsed-time))
	       (concepts (ambentry-resolved entries))
	       (subjects (ambentry-resolved
			  (pick entries ambentry-term title-names)))
	       (history
		(choice
		 (cons (if fastanalyze 'fastanalyze 'analyze) now)
		 (cons 'wordfetch (- done-wordfetch start-disambig))
		 (tryif fastanalyze
			(choice (cons 'disambigfetch
				      (- done-prefetch done-wordfetch))
				(cons 'disambigrun
				      (- done-disambig done-prefetch))
				(cons 'disambig
				      (- done-disambig done-wordfetch)))))))
	  (unless (string? item)
	    (drop! item '%history
		   (pick (get item '%history)
			 (if fastanalyze
			     '{fastanalyzeitem fastdisambig fastwordfetch}
			     '{analyzeitem disambig wordfetch})))
	    (add! item '%history history)
	    (when persist
	      (assert! item @?gn/concepts concepts)
	      (assert! item @?gn/subjects subjects))
	    (when index
	      (index-implied-values index item @?gn/concepts concepts)
	      (index-implied-values index item @?gn/subjects subjects)))
	  
	  (logger traceresults
	    "Found " (choice-size concepts) " concepts and "
	    (choice-size subjects) " subjects for " item
	    (if (or (exists? concepts) (exists? subjects)) "\n")
	    (do-choices (concept concepts)
	      (lineout traceoutput
		"\tCONCEPT=" concept " # "
		"wt=" (get-concept-score concept entries)
		", ap=" (get-concept-apriori concept scored-entries)
		", absfreq=" (absfreq concept)))
	    (do-choices (concept subjects)
	      (lineout traceoutput
		"\tSUBJECT=" concept " # "
		"wt=" (get-concept-score concept entries)
		", ap=" (get-concept-apriori concept scored-entries)
		", absfreq=" (absfreq concept))))
	  
	  (logger tracetime
	    "Found " (choice-size concepts) " concepts and "
	    (choice-size subjects) " subjects for " item " in "
	    (- done-disambig start-disambig) " seconds")
	  
	  (if (string? item)
	      (modify-frame title-analysis
		@?gn/subjects concepts @?gn/concepts concepts
		'%timing timing)
	      item))))))

;;;; Getting amb entries in various stages of refinement

;; This generates keyentries based on the titles and descriptions of
;; items as well as any assigned keywrods or the proper names
;; extracted by NLP analysis.
(define (refs/getentries item content-slotids subject-slotids (disambig #f))
  (let* ((language (try (get item @?gn/language) (get item 'language)
			default-language))
	 (title-analysis (nlp/analyze item subject-slotids #f))
	 (title-names (get title-analysis @?gn/proper-names))
	 (description-analysis (nlp/analyze item content-slotids #f))
	 (allnames (get item @?gn/proper-names))
	 (reduced-names (reduce-names allnames))
	 (full-names (pick reduced-names string?))
	 (name-map (pick reduced-names pair?))
	 (raw-entries
	  (ambentry/scored
	   (ambentry/given
	    (choice (term->ambentry full-names language 3)
		    (term->ambentry (get item @?gn/phrases) language)
		    (term->ambentry
		     (get item '{keywords @?gn/keywords}) language))))))
    (cond ((not disambig) raw-entries)
	  ((eq? disambig 'fast)
	   (ambentry/apriori-prefetch! raw-entries)
	   (ambentry/disambig raw-entries 0.6))
	  (else
	   (ambentry/apriori-prefetch! raw-entries)
	   (disambiguate-prefetch! raw-entries)
	   (disambiguate raw-entries -0.5)))))

;;;; Helper functions
    
(defambda (get-concept-score concept entries)
  (get (ambentry-scores (pick entries ambentry-resolved concept)) concept))

(defambda (get-concept-apriori concept entries)
  (largest (get (ambentry-scores (pick entries ambentry-possible concept)) concept)))

(define (absfreq concept) (choice-size (?? @?refterms concept)))
