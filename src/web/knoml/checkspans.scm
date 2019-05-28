;;; -*- Mode: Scheme; character-encoding: utf-8; -*-

;;; This file implements new virtual elements for FDXML files
;;;  on the sBooks site.
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

;;; This module defines FDXML handlers

(in-module 'knoml/checkspans)

(use-module '{texttools webtools xhtml})

(module-export! '{checkbox checkspan span.checkspan div.checkspan p.checkspan})

(define idpat #((subst "#" "") (not> {"." "#"})))
(define classpat #((subst "." "") (not> {"." "#"})))
(define tagpat #((bos) (isalpha) (isalnum+)))

(define (checkbox (type "CHECKBOX") (name #f) (value #f) (checked #f) (disabled #f) (%env))
  (let ((checkval (and checked (eval (string->lisp checked) %env)))
	(disval (and disabled (eval (string->lisp disabled) %env))))
    (input 'type type 'name name 'value (or value {})
	   (checked (if checked "CHECKED"))
	   (disabled (if disabled  "DISABLED")))))

(define (checkspan (spec #f) (type "CHECKBOX") (name #f) (value #f) (checked #f) (disabled #f) 
		   (%body '()) (%env))
  (let ((checkval (and checked (eval (string->lisp checked) %env)))
	(disval (and disabled (eval (string->lisp disabled) %env)))
	(tag (if spec (gather tagpat spec) "span"))
	(classes (tryif spec (gathersubst classpat spec))))
    (xmlstart tag 'class (glom "checkspan"
			   (and checkval " ischecked")
			   (and disval " disabled")
			   (and (exists? classes)
				(stringout (do-choices (class classes)
					     (printout " " class))))))
    (input 'type type 'name name 'value (or value {})
	   (checked (if checked "CHECKED"))
	   (disabled (if disabled  "DISABLED")))
    (xmlend tag)))

(define (span.checkspan (type "CHECKBOX") (name #f) (value #f) (checked #f) (disabled #f) 
			(%env) (%body))
  (let ((checkval (and checked (eval (string->lisp checked) %env)))
	(disval (and disabled (eval (string->lisp disabled) %env))))
    (span ((class (glom "checkspan"
		    (and checkval " ischecked")
		    (and disval " disabled"))))
      (input 'type type 'name name 'value (or value {}) 
	     (checked (if checked "CHECKED"))
	     (disabled (if disabled  "DISABLED")))
      (dolist (elt %body) (xmleval elt %env)))))

(define (div.checkspan (type "CHECKBOX") (name #f) (value #f) (checked #f) (disabled #f) 
		       (%env) (%body))
  (let ((checkval (and checked (eval (string->lisp checked) %env)))
	(disval (and disabled (eval (string->lisp disabled) %env))))
    (div ((class (glom "checkspan"
		   (and checkval " ischecked")
		   (and disval " disabled"))))
      (input 'type type 'name name 'value (or value {}) 
	     (checked (if checked "CHECKED"))
	     (disabled (if disabled  "DISABLED")))
      (dolist (elt %body) (xmleval elt %env)))))

(define (p.checkspan (type "CHECKBOX") (name #f) (value #f) (checked #f) (disabled #f) 
		     (%env) (%body))
  (let ((checkval (and checked (eval (string->lisp checked) %env)))
	(disval (and disabled (eval (string->lisp disabled) %env))))
    (p* ((class (glom "checkspan"
		  (and checkval " ischecked")
		  (and disval " disabled"))))
      (input 'type type 'name name 'value (or value {}) 
	     (checked (if checked "CHECKED"))
	     (disabled (if disabled  "DISABLED")))
      (dolist (elt %body) (xmleval elt %env)))))


