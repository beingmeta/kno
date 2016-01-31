;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

;;; Utilites for indexing XML content, especially XHTML
(in-module 'domutils/styles)

(use-module '{reflection fdweb xhtml texttools domutils varconfig logger})

(module-export! '{css/dropdecimals css/roundpixels css/relfonts
		  dom/normstyle dom/gather-styles!
		  css/relsizes})

(define-init %loglevel %notice%)

(define (dom/normstyle string (rules #f))
  (let* ((prepped (if rules (textsubst string (qc rules)) string))
	 (ruleset (for-choices (r (text->frames css-rule prepped))
		    (cons (stdspace (get r 'prop)) (stdspace (get r 'val)))))
	 (rules (rsorted ruleset car))
	 (prop #f))
    (if (> (length rules) 0)
	(stringout
	  (doseq (rule rules i)
	    (unless (equal? (car rule) prop)
	      (printout (if (> i 0) "; ")
		(car rule) ": " (trim-spaces (cdr rule)))
	      (set! prop (car rule))))
	  ";")
	"")))

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
	 (if (zero? (string->number (slice s 0 -2)))
	     "0px"
	     (glom (inexact->string  (/ (string->number (slice s 0 -2)) 16.0) 1)
	       "em")))
       (tryif (has-suffix s "pt")
	 (if (zero? (string->number (slice s 0 -2)))
	     "0pt"
	     (glom (inexact->string (/ (string->number (slice s 0 -2)) 12.0) 1)
	       "em")))))

(define (xform-font-size s)
  (try (fix-font-size s) s))

(matchdef! 'xform-font-size xform-font-size)

(define css/relfonts
  `(ic #({(spaces) "{"} "font-size:" (spaces*)
	 (subst {"small" "x-small" "xx-small"
		 "large" "x-large" "xx-large" "medium"
		 #((isdigit+) {"px" "pt"})
		 #((isdigit+) "." (isdigit+) {"px" "pt"})}
		xform-font-size)
	 (not> ";") {";" (eos)})))
(config! 'dom:cleanup:rules (cons 'css/relfonts css/relfonts))

(defambda (css/relsizes (properties {"font-size" "line-height" "text-indent"
				     "padding-left" "padding-right"
				     "padding-top" "padding-bottom"
				     "margin-left" "margin-right"
				     "margin-top" "margin-bottom"
				     "margin" "padding"}))
  `(ic {,(tryif (overlaps? "font-size" properties)
	   `#({(bos) (spaces) "{"} "font-size:" (spaces*)
	      (subst {"small" "x-small" "xx-small"
		      "large" "x-large" "xx-large" "medium"
		      #((isdigit+) {"px" "pt"})
		      #((isdigit+) "." (isdigit+) {"px" "pt"})}
		     ,xform-font-size)
	      (not> ";") {";" (eos)}))
	#({(bos) (spaces) "{"}
	  ,(glom (difference properties {"font-size" "margin" "padding"}) ":")
	  (spaces*)
	  (subst {"small" "x-small" "xx-small"
		  "large" "x-large" "xx-large" "medium"
		  #((isdigit+) {"px" "pt"})
		  #((isdigit+) "." (isdigit+) {"px" "pt"})}
		 ,xform-font-size)
	  (not> ";") {";" (eos)})
	,(tryif (overlaps? {"margin" "padding"} properties)
	   `#({(bos) (spaces) "{"} ,(glom (intersection properties {"margin" "padding"}) ":")
	      (spaces*)
	      (subst {#((isdigit+) {"px" "pt"})
		      #((isdigit+) "." (isdigit+) {"px" "pt"})}
		     ,xform-font-size)
	      (spaces*)
	      (not> ";") {";" (eos)}))}))

