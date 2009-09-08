;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'domutils/analyze)

(use-module '{tagger texttools textindex domutils domutils/index knowlets})

(module-export! 'dom/analyze)

;;; Module for analyzing text in a DOM
(define id "$Id: domtext.scm 4048 2009-06-20 15:18:16Z bemeta $")
(define revision "$Revision: 4048 $")

(define (get-table doc slotid)
  (try (get doc slotid)
       (let ((table (make-hashtable)))
	 (store! doc slotid table)
	 table)))

(defambda (dom/analyze doc (options #[]) (nodes) (index))
  (default! index (choice (get doc 'index) (getopt options 'index {})))
  (let* ((roots (choice (get doc 'indexroots) (getopt options 'indexroots {})))
	 (words (choice roots (try (get doc 'indexwords) (getopt options 'indexwords {}))))
	 (stops (choice (get doc 'stopwords) (getopt options 'stopwords {})))
	 (indexelts (->selector
		     (choice (getopt options 'indexelts {}) (get doc 'indexelts))))
	 (phrases (pick words compound?))
	 (knowlet (get (choice options doc) 'knowlet))
	 (phrasemaps (kno/phrasemap knowlet (try (get options 'language) 'en)))
	 (stopcache (get-table doc 'stopcache))
	 (rootcache (get-table doc 'rootcache))
	 (options
	  `#[textopts ,(getopt options 'textopts)
	     phrases ,phrases
	     phrasemap ,(choice (get options 'phrasemaps) phrasemaps)
	     roots ,(choice roots (get options 'roots))
	     refrules ,(get options 'refrules)
	     words ,(choice words (get options 'words))
	     textfns ,dom/textify
	     stops ,(choice stops (get options 'stops))
	     stopcache ,stopcache
	     rootcache ,rootcache]))
    (%watch options)
    (let* ((cacheslots (getopt options 'cacheslots #t))
	   (textnodes (if (bound? nodes) nodes (dom/find doc indexelts)))
	   (keystrings (text/analyze textnodes options)))
      (do-choices (node (pickoids textnodes))
	(let* ((keys (get keystrings node))
	       (terms (pickstrings keys))
	       (pairs (pick keys pair?))
	       (fields (if (test options 'textopts 'keepraw) (car pairs)
			   (difference (car pairs) 'words))))
	  (when (or (eq? cacheslots #t) (overlaps? 'terms cacheslots))
	    (add! node 'terms terms))
	  (when index
	    (index-frame index node 'terms terms)
	    (when (exists? terms) (index-frame index node 'has 'terms))
	    (index-frame index node 'has fields)
	    (do-choices (field fields)
	      (index-frame index node field (get pairs field))))
	  (do-choices (field (if (eq? cacheslots #t) fields
				 (intersection field cacheslots)))
	    (add! node field (get pairs field)))))
      keystrings)))









