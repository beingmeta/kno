;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

;;; Utilites for indexing XML content, especially XHTML
(in-module 'domutils/styles)

(use-module '{reflection fdweb xhtml texttools domutils varconfig logger})

(module-export! '{css/dropdecimals dom/normstyle dom/gather-styles!})

(define %loglevel %notice%)

(define css/dropdecimals
  {#((isdigit) (isdigit+) (subst #("." (isdigit+)) "") "px")
   #((isdigit) (isdigit+) (subst #("." (isdigit+)) "") "pt")
   #({"5" "6" "7" "8" "9"} (subst #("." (isdigit+)) "") "px")
   #({"5" "6" "7" "8" "9"} (subst #("." (isdigit+)) "") "pt")
   #((subst "0.00" "0") {"pt" "px"})})

(define css-rule
  #({(bos) ";"} (spaces*)
    (label prop (+ {(isalnum) "-"})) ":" (spaces*)
    (label val (not> {";" (eos)}))))

(define (dom/normstyle string (rules #f))
  (let* ((prepped (if rules
		      (textsubst (decode-entities string) (qc rules))
		      (decode-entities string)))
	 (ruleset (for-choices (r (text->frames css-rule prepped))
		    (cons (get r 'prop) (trim-spaces (get r 'val)))))
	 (rules (rsorted ruleset car))
	 (prop #f))
    (stringout
      (doseq (rule rules i)
	(unless (equal? (car rule) prop)
	  (printout (if (> i 0) "; ")
	    (car rule) ": " (trim-spaces (cdr rule)))
	  (set! prop (car rule))))
      ";")))

(define (dom/gather-styles! node (stylemap (make-hashtable)) (normalize #t))
  (when (test node 'style)
    (let ((norm (if normalize
		    (dom/normstyle (get node 'style)
				   (if (eq? normalize #t) #f normalize))
		    (get node 'style))))
      (dom/set! node 'style norm)
      (add! stylemap norm node)))
  (when (test node '%content)
    (doseq (child (->vector (get node '%content)))
      (dom/gather-styles! child stylemap normalize)))
  stylemap)

