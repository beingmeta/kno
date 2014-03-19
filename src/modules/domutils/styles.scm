;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

;;; Utilites for indexing XML content, especially XHTML
(in-module 'domutils/styles)

(use-module '{reflection fdweb xhtml texttools domutils varconfig logger})

(module-export! '{css/dropdecimals css/roundpixels css/relfonts
		  dom/normstyle dom/gather-styles!})

(define %loglevel %notice%)

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
      (if (empty-string? norm)
	  (dom/drop! node 'style)
	  (dom/set! node 'style norm))
      (unless (empty-string? norm) (add! stylemap norm node))))
  (when (test node '%content)
    (doseq (child (->vector (get node '%content)))
      (unless (string? child)
	(dom/gather-styles! child stylemap normalize))))
  stylemap)

;;; Dropping decimals

(define css/roundpixels
  {#((isdigit) (isdigit+) (subst #("." (isdigit+)) "") "px")
   #((isdigit) (isdigit+) (subst #("." (isdigit+)) "") "pt")
   #({"5" "6" "7" "8" "9"} (subst #("." (isdigit+)) "") "px")
   #({"5" "6" "7" "8" "9"} (subst #("." (isdigit+)) "") "pt")
   #((subst "0.00" "0") {"pt" "px"})})
(define css/dropdecimals css/roundpixels)
(config! 'dom:cleanup:rules (cons 'css/roundpixels css/roundpixels))

(define css-rule
  #({(bos) ";"} (spaces*)
    (label prop (+ {(isalnum) "-"})) ":" (spaces*)
    (label val (not> {";" (eos)}))))

;;; Replacing CSS relative font sizes

(define named-sizes
  #["medium" "1.0em"
    "large" "1.2em" "x-large" "1.5em" "xx-large" "1.8em"
    "small" "0.8em" "x-small" "0.6em" "xx-small" "0.45em" ])

(define (fix-font-size s)
  (try (get named-sizes s)
       (tryif (has-suffix s "px")
	 (glom (inexact->string  (/ (string->number (slice s 0 -2)) 16.0) 2)
	   "em"))
       (tryif (has-suffix s "pt")
	 (glom (inexact->string (/ (string->number (slice s 0 -2)) 12.0) 2)
	   "em"))))

(define (xform-font-size s)
  (try (fix-font-size s) s))

(define css/relfonts
  `(ic #({(spaces) "{"} "font-size:" (spaces*)
	 (subst {"small" "x-small" "xx-small"
		 "large" "x-large" "xx-large" "medium"
		 #((isdigit+) {"px" "pt"})
		 #((isdigit+) "." (isdigit+) "px")}
		,xform-font-size)
	 (not> ";") ";")))
(config! 'dom:cleanup:rules css/relfonts)
