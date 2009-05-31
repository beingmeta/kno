;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2009 beingmeta, inc.  All rights reserved.

(in-module 'knowlets/drules)

;;; Disambiguation rules for knowlets
;;;  Designed to work with a full-text inverted index
;;;   like those produced by textindex
(define id "$Id$")
(define revision "$Revision$")

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knowlets})

(module-export!
 '{kno/drule
   kno/apply-drule! kno/apply-drules!
   drule-language drule-subject
   drule-cues drule-context+ drule-context-
   drule-threshold})

(defrecord DRULE
  subject
  language
  cues
  context+
  (context- {})
  (threshold 1)
  (knowlet #f))

(defambda (kno/drule subject (language) (cues) (context+ {})
		     (context- {}) (threshold 1)
		     (knowlet))
  (default! knowlet (get subject 'knowlet))
  (default! language (knowlet-language knowlet))
  (default! cues (get subject language))
  (cons-drule subject language cues context+ context- threshold
	      knowlet))

(define (run-gauntlet candidate gauntlet threshold (i 0) (count 0))
  (when (< threshold 1)
    (set! threshold (floor (* (length gauntlet) threshold))))
  (or (>= count threshold)
      (and (< i (length gauntlet))
	   (if (overlaps? candidate (elt gauntlet i))
	       (run-gauntlet candidate gauntlet threshold
			     (1+ i) (1+ count))
	       (run-gauntlet candidate gauntlet threshold
			     (1+ i) count)))))

(define (kno/apply-drule! drule index (idmap #f)
			  (tagslot 'tags)
			  (wordslots '{words terms})
			  (tagslots))
  (default! tagslots tagslot)
  (let* ((matches
	  (choice (find-frames index wordslots (pickstrings (drule-cues drule)))
		  (find-frames index tagslots (pickoids (drule-cues drule)))))
	 (excluded
	  (choice (find-frames index
		    wordslots (pickstrings (drule-context- drule)))
		  (find-frames index
		    tagslots (pickoids (drule-context- drule)))))
	 (surviving (difference matches excluded))
	 ;; The gauntlet is a vector of all candidates matching
	 ;;  the positive contexts.  A candidate must be in at least
	 ;;  N (the threshold) elements of the vector to pass.
	 (gauntlet (map (lambda (val)
			  (if (oid? val)
			      (find-frames index tagslots val)
			      (find-frames index wordslots val)))
			(sorted (drule-context+ drule))))
	 (elected
	  (if (= (length gauntlet) 0)
	      surviving
	      (if (number? (drule-threshold drule))
		  (filter-choices (c surviving)
		    (run-gauntlet c gauntlet (drule-threshold drule)))
		  (if (drule-threshold drule)
		      (filter-choices (c surviving)
			(every? (lambda (x) (overlaps? c x)) gauntlet))
		      (filter-choices (c surviving)
			(some? (lambda (x) (overlaps? c x)) gauntlet)))))))
    (add! (if idmap (get idmap elected) elected)
	  slotid (drule-subject drule))
    (add! index (cons slotid (drule-subject drule))
	  elected)
    (add! index (cons 'has slotid) elected)))

(define (kno/apply-drules! knowlet index (idmap #f) (slotid 'concepts))
  (let* ((words (get (getkeys index) '{words roots refs}))
	 (drules (get (knowlet-drules knowlet) words)))
    (do-choices (drule drules)
      (kno/apply-drule! drule index idmap slotid))))

