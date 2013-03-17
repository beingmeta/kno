(in-module 'gnosys/taghub)

;;; This file contains functions for mapping concepts into tags using
;;; the gnosys/tagsites module.
;;;  Copyright (C) 2007-2013 beingmeta, inc.

(define version "$Id$")

(use-module '{texttools fdweb})
(use-module '{ezrecords cachequeue meltcache logger})
(use-module '{brico brico/lookup})
(use-module '{gnosys gnosys/tagsites})

(define %loglevel %debug!)

(define tag-queue #f)
(defslambda (init-tagqueue)
  (unless tag-queue
    (set! tag-queue
	  (cachequeue (config 'tagcache (make-hashtable))
		      (config 'tagfetchthreads 4)
		      (config 'tagmeltpoint (* 20 60))))))

;;;; Checking whether things are tags

;;(define istag-cache (open-index (mkpath datadir "istag.index")))
(define istag-cache (make-hashtable))

(define (istag? tag)
  (unless tag-queue (init-tagqueue))
  (and
   ;; Require that every term in a compound also be a tag
   (or (= (length (tag-terms tag)) 1)
       (every? istag? (map (lambda (t) (make-tag  (tag-siteid tag) t))
			   (tag-terms tag))))
   (if (exists? (cq/info tag-queue tag/%getitems tag))
       ;; If the tag has been cached and it has items, take that
       ;;  as evidence that the tag exists.
       (or (exists? (cq/probe tag-queue tag/%getitems tag))
	   (exists? (cq/get tag-queue tag/%getitems tag)))
       ;; otherwise, do a get directly
       (exists? (cq/get tag-queue tag/%getitems tag)))))

(define (melt/istag? tag)
  (unless tag-queue (init-tagqueue))
  (and
   ;; Require that every term in a compound also be a tag
   (or (= (length (tag-terms tag)) 1)
       (every? istag? (map (lambda (t) (make-tag  (tag-siteid tag) t))
			   (tag-terms tag))))
   (if (exists? (cq/info tag-queue tag/%getitems tag))
       ;; If the tag has been cached and it has items, take that
       ;;  as evidence that the tag exists and will do so for a long time
       (meltentry #f (exists? (cq/probe tag-queue tag/%getitems tag))
		  (* 7 24 60 60))
       ;; otherwise, do a get directly to queue the request but make
       ;; it short-lived.
       (meltentry #f (exists? (cq/get tag-queue tag/%getitems tag))
		  7))))

(define (tagitems tag)
  (unless tag-queue (init-tagqueue))
  (and
   ;; Require that every term in a compound also be a tag
   (or (= (length (tag-terms tag)) 1)
       (every? istag? (map (lambda (t) (make-tag  (tag-siteid tag) t))
			   (tag-terms tag))))
   (cq/get tag-queue tag/%getitems tag)))


;;;; Good base terms

(define (good-base-term? concept language term)
  (let* ((meanings (?? language term))
	(n-meanings (choice-size meanings))
	(sum-freqs (reduce-choice + meanings 0 absfreqs)))
    (> (get absfreqs concept)
       (/ sum-freqs (min 10 n-meanings)))))

(define (getasbfreq concept) (try (1+ (get absfreqs concept)) 1))
(define (good-single-term? concept language term)
  (let* ((meanings (?? language term))
	 (n-meanings (choice-size meanings))
	 (thisfreq (getabsfreq concept))
	 (sum-freqs (reduce + (map getabsfreq (choice->vector meanings)))))
    ;; Over 1/3 of references are to this concept
    (> (* thisfreq 3) sum-freqs)))
(define (very-good-single-term? concept language term)
  (let* ((meanings (?? language term))
	 (n-meanings (choice-size meanings))
	 (thisfreq (getabsfreq concept))
	 (sum-freqs (reduce + (map getabsfreq (choice->vector meanings)))))
    ;; Over 90% of references are to this concept
    (> (* (get absfreqs concept) 1.333) sum-freqs)))

(define (good-single-term concept language term)
  (tryif (good-single-term? concept language term)
	 concept))

(module-export! '{good-single-term? very-good-single-term? good-single-term})


;;;; Finding combos

(defambda (find-combos concept (language @?en) (terms #f))
  (let ((terms (or terms (get concept language))))
    (for-choices (term terms)
      (if (singleton? (?? language term)) (fail)
	  (vector term
		  (choice (for-choices (gterm (get-norm (get concept @?implies) language))
			    (tryif (singleton? (?? language term @?implies (?? language gterm)))
				   gterm))
			  (for-choices (gterm (get-norm (get concept '{@?partof region country}) language))
			    (tryif (singleton? (?? language term @?partof* (?? language gterm)))
				   gterm))
			  (for-choices (gterm (get-norm (get concept @?sumterms) language))
			    (tryif (singleton? (?? language term @?sumterms (?? language gterm)))
				   gterm))))))))

(define (get-tagvecs concept (language english) (justnorm #f))
  (for-choices (word (if justnorm (get-norm concept language)
			 (choice (pick (get concept language) length > 2)
				 (get concept 'lastname))))
    (let ((variants (choice word (downcase word) (capitalize word))))
      (if (identical? (?? language variants) concept)
	  (vector word)
	  (choice
	   (tryif (or (identical? (?? (get norm-map language)
				      (choice word (downcase word) (capitalize word)))
				  concept)
		      (good-single-term? concept language word))
		  (vector word))
	   (tryif (and (exists? absfreqs)
		       (good-base-term? concept language word)
		       (not (very-good-single-term? concept language word)))
		  (cachepoint find-combos concept language word)))))))

(define default-sites '{delicious technorati flickr youtube})

(define (findtags concept (sites default-sites))
  (unless tag-queue (init-tagqueue))
  (%debug "Finding tags for " concept " on " sites)
  (let ((direct-tags (tagify (get-tagvecs concept @?en #t) sites))
	(indirect-tags (tagify (get-tagvecs concept @?en) sites)))
    (%debug "Prefetch for " (choice-size indirect-tags) " indirect tags")
    (cq/prefetch tag-queue tag/%getitems indirect-tags)
    (%debug "Requiring " (choice-size direct-tags) " direct tags")
    (cq/require tag-queue tag/%getitems direct-tags)
    (%debug "Require done for " (choice-size direct-tags) " tags")
    (pick indirect-tags istag?)))

(define (findtags/prefetch concept (sites default-sites))
  (let ((direct-tags (tagify (get-tagvecs concept @?en #t) sites))
	(indirect-tags (tagify (get-tagvecs concept @?en) sites)))
    (cq/prefetch tag-queue tag/%getitems indirect-tags)
    (cq/request tag-queue tag/%getitems direct-tags)
    (cq/request tag-queue tag/%getitems
		(difference indirect-tags direct-tags))))

(module-export!
 '{istag?
   melt/istag?
   init-tagqueue
   tagitems tag-queue
   get-tagvecs findtags findtags/prefetch
   good-base-term?})




