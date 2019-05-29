;;; -*- Mode: Scheme; -*-

;;; Copyright (C) 2005-2006 beingmeta, inc.
;;; This program code is proprietary to and a valuable trade secret of 
;;;   beingmeta, inc.  Disclosure or distribution without explicit
;;;   approval is forbidden.

(in-module 'gnosys/metakeys/disambiguate)

(use-module '{reflection brico morph})
(use-module '{gnosys gnosys/metakeys})

(define trace-disambig #f)

;;; New design:
;;;  * works without corpus, using ontology
;;;  * can use corpus information
;;;  * incorporates other arbitrary rules
;;;
;;;  Simple model:
;;;   ap(w,l,c) is an a priori distribution, where
;;;    w is a word, l is a language, and c is a concept
;;;   Expanders map concepts into other related concepts.
;;;   E is a set of expanders, and weight(x,e,i) is
;;;    a weight associated with the expansion x=e(i).

;;;   The algorithm starts with a bunch of possible concepts and
;;;    computes an initial set of apriori scores.  It then
;;;    expands each of those concepts to other concepts and
;;;    credits those other concepts based on the a priori scores

;;;   cs(c) is a score function over concepts,
;;;    over w in words, c in concepts, e in expanders,
;;;      over o in e(c)
;;;        cs(o)=cs(o)+ap(w,l,c)*weight(o,e,c)
;;;   then, over w, return c with max cs
;;;   Expanders, for example, take wholes and expand to parts
;;;    or parts and expand to wholes, kinds to kindof, etc.
;;;   Problem is that disambiguation does a lot of fetching

;;;  Another possible implementation:
;;;   Replace expansion with search.
;;;   Generate a pool of concepts scored a priori
;;;   Compute a second set of scores based on
;;;     search functions and associated weights.
;;;   Can do this by using search in the expanders
;;;    and making cs (above) initialized to zero for possible
;;;    concepts and use increment-if-existing.

;;; Another new design:
;;;   Just as before, a path method takes a node and a set of other nodes
;;;    and identifies weighted links to the other nodes.
;;;   In the new model, a path consists of the source (implied), a type (the path method),
;;     a target, a weight, and a "justification" which is a node or set of nodes.
;;;   Path weights are maximized over the type and justificiation.
;;;   Proposal 1:
;;;   To score a concept C, iterate over all the entries in the context except those that
;;;    include C.  For each entry E with possible meanings P, generate all of the paths
;;;    between C and P.  Maximize all the paths with the same method and justification and
;;;    add together the resulting maximums, multiplying the sum by the a priori weight of P.
;;;   Proposal 2:
;;;   To score a concept C, iterate over all non-competing elements of the context D.  For each
;;;    D, generate the paths from C to D, multiplying the path weight by apriori(D) and maximizing
;;;    the path weights for all entries with the same method and justification.  The score for the
;;;    concept is the sum of these maximized path weights.  Justifications are either OIDs or choices
;;;    of OIDs and identity is taken to be overlap.
;;;   Implementation thoughts:
;;;    You could keep a table of <D,M,J> vectors and maximize the values, then add up the values
;;;     for a given D at the end.

;;; Here's the trick.  You want any piece of evidence to only count once 

;;; This takes a possibly weighted expansion and combines its weight
;;; with the method weight.
(define (combine-weights hint weight)
  (if (pair? hint)
      (cons (car hint) (* (cdr hint) weight))
      (cons hint weight)))

(define (inv-weight slotid value)
  "Returns a number in [0,1] based on the frequency of a given slot value"
  ;; We use max to avoid negative values with incredibly common slot values
  (max 0.0 (- 1.0 (* .05 (ilog (choice-size (?? slotid value)))))))

;;; The core disambiguation algorithm

(define default-path-methods '())
;; (vector 'DEFTERMSCORRELATE 4 correlator @?defterms #f)
;; (vector 'REFTERMSCORRELATE 2 correlator @?refterms #f)
;; (vector 'PARTOFPATH 4 @?partof*)
;; (vector 'IMPLIESMATCH 4 coincidences @?implies)

(defambda (combine-apriori entries)
  (let ((combined (make-hashtable)))
    (do-choices (scores (pick (keyentry-scores entries) table?))
      (do-choices (key (getkeys scores))
	(let ((score (get scores key))
	      (max (try (get combined key) 0)))
	  (when (> score max) (store! combined key score)))))
    combined))

(define (find-paths method source context)
  "Returns the paths from source to dest.  Each path is a list
   consisting of a target, a weight, and a justification."
  (let ((handler (third method)) (len (length method)))
    (cond ((slotid? method)
	   (for-choices (cxtelt context)
	     (tryif (overlaps? source (?? handler cxtelt))
		    (vector (inv-weight handler cxtelt) cxtelt cxtelt))))
	  ((not (applicable? handler)) (fail))
	  ((= len 3)
	   (if (= (fcn-arity method) 1)
	       (vector weight (qc (intersection (handler source) context)) #f)
	       (handler source (qc context))))
	  ((= len 4) (handler source (qc context) (fourth method)))
	  ((= len 5) (handler source (qc context) (fourth method) (fifth method)))
	  (else (apply handler source (qc context) (->list (subseq method 3)))))))

(defambda (get-pathscores concept entries apriori methods)
  (for-choices (concept concept)
    (let* ((justifications (make-hashtable))
	   (context (keyentry-candidates (reject entries keyentry-candidates concept))))
      (doseq (method methods)
	(let ((paths (find-paths method concept (qc context))))
	  (do-choices (path paths)
	    (let* ((target (second path))
		   (score (* (first path) (largest (get apriori target))))
		   (key (vector (third path) (first method)))
		   (current (try (get justifications key) 0)))
	      (when (> score current) (store! justifications key score))))))
      justifications)))

(defambda (contextual-score entries (justify #f) (methods default-path-methods))
  (let* ((scores (make-hashtable))
	 (entries (keyentry/scored entries))
	 (candidates (keyentry-candidates entries))
	 (apriori (combine-apriori entries)))
    (do-choices (candidate candidates)
      (let* ((pathscores (get-pathscores candidate entries apriori methods))
	     (paths (getkeys pathscores))) ;; (sum 0)
	(store! scores candidate (reduce-choice + paths 0 pathscores))
	(when justify
	  (do-choices (path paths)
	    (add! scores (list candidate)
		  (cons (get pathscores path) path))))))
    scores))

(module-export! '{contextual-score find-paths get-pathscores combine-apriori})

;;; Actual disambiguation

(defambda (disambiguate entries (threshold #t))
  (let* ((entries (keyentry/scored (keyentry/useknown entries)))
	 (cxtscore (contextual-score entries))
	 (biased (keyentry/bias entries cxtscore)))
    (keyentry/disambig entries threshold)))

(module-export! 'disambiguate)

;;; Keylist functions (legacy?)

;;; This disambiguates a keylist using contextual scoring.
(define (disambiguate-keylist keylist (threshold #t))
  (let ((scores (contextual-score (elts (keylist-entries keylist)))))
    (vector (qc (keylist-text keylist)) (qc (keylist-language keylist))
	    (map (lambda (e) (keyentry/pick e scores threshold))
		 (keylist-entries keylist)))))

;;; This disambiguates a keylist using contextual scoring.
(define (contextual-score-keylist keylist)
  (contextual-score (elts (keylist-entries keylist))))

(module-export! '{disambiguate-keylist contextual-score-keylist})

;;; Coincidence path methods

(define (coincidences c o slotid)
  (largest
   (for-choices (v (choice c (get c slotid)))
     (let ((coincidence (intersection (?? slotid v) o)))
       (tryif (exists? coincidence)
	      (vector (pick> (* (inv-weight slotid v) (max 1 (ilog (choice-size coincidence)))) 0)
		      (qc coincidence) v))))
   first))
      
(module-export! 'coincidences)

(set! default-path-methods
      (cons* (vector 'IMPLIESMATCH 3 coincidences implies)
 	     default-path-methods))

;;; Correlation path methods

(define (correlate c1 c2 slotid (index #f))
  (let ((c1hits (if index (find-frames index slotid c1) (?? slotid c1)))
	(c2hits (if index (find-frames index slotid c2)
		    (?? slotid c2)))
	(c1dhits (if index (find-frames index slotid (list c1))
		     (?? slotid (list c1))))
	(c2dhits (if index (find-frames index slotid (list c2))
		     (?? slotid (list c2)))))
    (let ((c1size (choice-size c1hits))
	  (c2size (choice-size c2hits))	  
	  (cosize (choice-size (intersection c1hits c2hits))))
      (if (or (zero? c1size) (zero? c2size)) 0
	  (/~ (* cosize cosize) (* c1size c2size))))))

(define (correlator c others slotid (index #f))
  (let ((correlations (make-hashtable))
	(sum 0.0))
    (tryif (exists? (if index (find-frames index slotid c) (?? slotid c)))
	   (for-choices (other others)
	     (tryif (exists? (if index (find-frames index slotid other) (?? slotid other)))
		    (vector (pick> (correlate c other slotid index) 0.0001)
			    other (smallest (if index (find-frames index slotid c slotid other)
						(?? slotid c slotid other)))))))))

;; (define (correlator c others slotid (index #f))
;;   (let ((correlations (make-hashtable))
;; 	(sum 0.0))
;;     (for-choices (other others)
;;       (vector (pick> (correlate c other slotid index) 0.0001)
;; 	      other (smallest (if index (find-frames index slotid c slotid other)
;; 				  (?? slotid c slotid other)))))))
(module-export! '{correlate correlator})

(set! default-path-methods
      (cons* (vector 'DEFTERMSCORRELATE 8 correlator defterms #f)
	     (vector 'REFTERMSCORRELATE 4 correlator refterms #f)
	     default-path-methods))

;;; Prefetching

(define (get-path-args handler i (methods default-path-methods))
  (if (null? methods) (fail)
      (choice 
       (tryif (eq? (third (car methods)) handler) (elt (car methods) i))
       (get-path-args handler i (cdr methods)))))

(define disambiguate-prefetch!
  (ambda (entries (refcheck #f) (methods default-path-methods))
    (let ((oids (keyentry-candidates entries))
	  (path-slotids (pick (elts (map third methods)) oid?))
	  (coincidence-slotids (get-path-args coincidences 3 methods))
	  (correlation-slotids (get-path-args correlator 3 methods))
	  (wfbases (tryif (config 'usewordforms #t)
			  (keyentry-base entries)))
	  (wflanguages (tryif (config 'usewordforms #t)
			      (keyentry-language entries))))
      (comment
       (message "path-slotids=" path-slotids)
       (message "coincidence-slotids=" coincidence-slotids)
       (message "correlation-slotids=" correlation-slotids))
      (prefetch-oids! oids)
      (when refcheck
	;; Do these prefetches when 'refchecking' whether prefetching
	;;  gets everything.  Practically, these are one-time fetches
	;;  that don't grow with the set, so it's not a pain to fetch them.
	(prefetch-oids! coincidence-slotids)
	(prefetch-oids! (get coincidence-slotids '{inverse slots}))
	(prefetch-oids! (get (pick (get coincidence-slotids 'slots) oid?) 'inverse)))
      (when (config 'usewordforms #t)
	(prefetch-oids! (choice (?? 'language wflanguages)
				(?? 'word wfbases)
				(?? 'oid oids))))
      (prefetch-keys!
       (choice 
	;; These are for frequency information
	(tryif (config 'usewordforms #t)
	       (choice (cons 'language wflanguages)
		       (cons 'word wfbases)
		       (cons 'of oids)))
	;; For simple slotid paths
	(cons path-slotids oids)
	;; For correlation paths
	(cons correlation-slotids oids)
	(cons correlation-slotids (list oids))
	;; For coincidence paths
	(cons coincidence-slotids oids)
	;; For computing the coincidence slots themselves, when they have
	;;  inverses
	(cons (choice (get (pick coincidence-slotids oid?) 'inverse)
		      (get (pick (get (pick coincidence-slotids oid?) 'slots) oid?)
			   'inverse))
	      oids)))
      ;; Now get the indcies we look up
      (prefetch-keys! (cons coincidence-slotids (get oids coincidence-slotids))))))

(module-export! 'disambiguate-prefetch!)

(begin ;; Replace with begin and eval to define
 (use-module 'trackrefs)
 (define (refcheck-disambig entries)
   (clearcaches) (disambiguate-prefetch! entries #t)
   (trackrefs (lambda () (disambiguate entries))))
 (module-export! 'refcheck-disambig))

