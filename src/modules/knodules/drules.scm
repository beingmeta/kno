;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'knodules/drules)

;;; Disambiguation rules for knodules
;;;  Designed to work with a full-text inverted index
;;;   like those produced by textindex

(use-module '{texttools fdweb ezrecords varconfig})
(use-module '{knodules})

(module-export!
 '{kno/drule
   kno/disambiguate! kno/apply-drule! kno/apply-drules!
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
  (knodule #f))

(defambda (kno/drule subject (language) (cues) (context+ {})
		     (context- {}) (threshold 1)
		     (knodule))
  (default! knodule (get subject 'knodule))
  (default! language (knodule-language knodule))
  (default! cues (get subject language))
  (cons-drule subject language cues context+ context- threshold
	      knodule))

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
	  (tryif (exists? matches)
	    (choice (find-frames index
		      wordslots (pickstrings (drule-context- drule)))
		    (find-frames index
		      tagslots (pickoids (drule-context- drule))))))
	 (surviving (difference matches excluded))
	 (gauntlet (tryif (exists? surviving)
		     (map (lambda (val)
			    (if (oid? val)
				(find-frames index tagslots val)
				(find-frames index wordslots val)))
			  (sorted (drule-context+ drule))))))
    (tryif (and (exists? surviving) (exists? gauntlet)
		(not (position 0 (map choice-size gauntlet))))
      ;; The gauntlet is a vector of all candidates matching
      ;;  the positive contexts.  A candidate must be in at least
      ;;  N (the threshold) elements of the vector to pass.
      (let ((elected
	     (if (= (length gauntlet) 0) surviving
		 (if (number? (drule-threshold drule))
		     (filter-choices (c surviving)
		       (run-gauntlet c gauntlet (drule-threshold drule)))
		     (if (drule-threshold drule)
			 (filter-choices (c surviving)
			   (every? (lambda (x) (overlaps? c x)) gauntlet))
			 (filter-choices (c surviving)
			   (some? (lambda (x) (overlaps? c x)) gauntlet)))))))
	(cond ((hashtable? tagslot)
	       (add! tagslot (if idmap (get idmap elected) elected)
		     (drule-subject drule)))
	      (else (add! (if idmap (get idmap elected) elected)
			  tagslot (drule-subject drule))
		    (add! index (cons tagslot (drule-subject drule))
			  elected)
		    (add! index (cons 'has tagslot) elected)))))))

(define (kno/apply-drules! knodule index (idmap #f) (slotid 'concepts))
  (let* ((words (get (getkeys index) '{words roots refs}))
	 (drules (get (knodule-drules knodule) words)))
    (do-choices (drule drules)
      (kno/apply-drule! drule index idmap slotid))))

(define (kno/disambiguate! index knodule settings)
  (let* ((wordslots (getopt settings 'wordslots '{words roots terms refs}))
	 (tagslots (getopt settings 'tagslots '{tags concept}))
	 (tagslot (getopt settings 'tagslot
			  (if (overlaps? tagslots 'tags) 'tags
			      (pick-one tagslots))))
	 (idmap (try (get settings 'idmap) #f))
	 (saveto (try (get settings 'saveto) (make-hashtable)))
	 (words (get (getkeys index) wordslots))
	 (drules (get (knodule-drules knodule) words))
	 (knoindex (knodule-index knodule))
	 (unknown (reject words (knodule-drules knodule)))
	 (language (try (get settings 'language) (knodule-language knodule)))
	 (knownterms (get (getkeys knoindex) language))
	 (possible (intersection unknown knownterms))
	 (newterms {}))
    (set+! tagslots tagslot) ;; just in case
    (do-choices (drule drules)
      (kno/apply-drule! drule index idmap (qc tagslots)
			(qc wordslots) (qc tagslots)))
    (do-choices possible
      (let* ((dterms (find-frames knoindex language possible))
	     (dterm (singleton dterms))
	     (elected (find-frames index wordslots possible)))
	(when (fail? dterm)
	  (set! dterm (make-union-dterm possible language knodule dterms))
	  (set+! newterms dterm))
	(cond ((hashtable? saveto)
	       (add! saveto
		     (if idmap (get idmap elected) elected)
		     elected))
	      (else (add! (choice (pickoids elected)
				  (if idmap (get idmap elected) elected))
			  saveto dterm)
		    (add! index (cons saveto dterm) elected)
		    (add! index (cons 'has saveto) elected)))))
    newterms))

(defambda (make-union-dterm term language knodule terms)
  (let ((dterm (kno/dterm (stringout term "(ambiguous)") knodule)))
    (kno/add! dterm language term)
    (kno/add! dterm 'specls terms)
    dterm))

