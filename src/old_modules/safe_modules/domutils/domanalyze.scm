;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Natural language analysis (with textindex) of DOM nodes

(in-module 'domutils/analyze)

(use-module '{textindex domutils logger})

(define-init %loglevel %notice%)

(module-export! 'dom/analyze)

;;; Module for analyzing text in a DOM

(define (get-table doc slotid)
  (try (get doc slotid)
       (let ((table (make-hashtable)))
	 (store! doc slotid table)
	 table)))

(defambda (dom/analyze doc (options #[]) (nodes) (index))
  (default! index (choice (get doc 'index) (getopt options 'index {})))
  (let* ((words (choice (get doc 'indexroots)
			(getopt options 'indexwords {})))
	 (stops (choice (get doc 'stopwords) (getopt options 'stopwords {})))
	 (textelts
	  (->selector
	   (choice (getopt options 'textelts {}) (get doc 'textelts))))
	 (nontextelts (->selector
		       (choice (getopt options 'nontextelts {})
			       (get doc 'nontextelts))))
	 (stopcache (get-table doc 'stopcache))
	 (rootcache (get-table doc 'rootcache))
	 (justwords (pickstrings words))
	 (wordtables (pick (reject words string?) {hashtable? hashset?}))
	 (options
	  (cons
	  `#[rootset
	     ,(if (exists? words)
		  (choice (choice->hashset justwords) wordtables)
		  {})
	     phrasemap
	     ,(choice (getopt options 'phrasemap {})
		      (text/phrasemap (pick justwords compound?)
				      wordtables))
	     textfns ,dom/textify
	     stops ,stops
	     stopcache ,stopcache
	     rootcache ,rootcache]
	  options)))
    (default! nodes
      (difference (try (dom/find doc textelts)
		       (pick (find-frames index 'has 'id) dom/textual?))
		  (dom/find doc nontextelts)))
    (debug%watch "DOM/ANALYZE" options)
    (let* ((cacheslots (choice (getopt options 'cacheslots #t)
			       (tryif (testopt options 'textopts 'keepraw)
				 'words)
			       'terms))
	   (analysis (text/analyze nodes options)))
      (do-choices (node (pickoids nodes))
	(let* ((keys (get analysis node))
	       (terms (pickstrings keys))
	       (pairs (pick keys pair?))
	       (fields  (if (or (overlaps? cacheslots #t)
				(overlaps? cacheslots 'all))
			    (car pairs)
			    (intersection (car pairs) cacheslots))))
	  (debug%watch "DOM/ANALYZE" node terms pairs)
	  (when (or (eq? cacheslots #t) (overlaps? 'terms cacheslots))
	    (add! node 'terms terms))
	  (when index
	    (index-frame index node 'terms terms)
	    (when (exists? terms) (index-frame index node 'has 'terms))
	    (index-frame index node 'has fields)
	    (do-choices (field fields)
	      (index-frame index node field (get pairs field))))
	  (index-frame index node 'has fields)
	  (unless (overlaps? cacheslots #f)
	    (do-choices (field fields)
	      (add! node field (get pairs field))))))
      (store! doc 'analysis analysis)
      analysis)))




