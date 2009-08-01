;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2006-2009 beingmeta, inc.  All rights reserved.

(in-module 'deeptags)

(define version "$Id: pings.scm 4101 2009-07-02 21:08:28Z bemeta $")
(define revision "$Revision: 4101 $")

(use-module '{texttools reflection})
(use-module '{varconfig ezrecords})
(use-module '{brico brico/lookup brico/dterms})
(use-module '{knowlets})

(defrecord tag term (oid {}))

(define (tag->oid tag)
  (if (oid? tag) tag
      (if (string? tag)
	  (if (has-prefix tag "@")
	      (string->lisp tag)
	      (fail))
	  (if (tag? tag) (tag-oid tag) (fail)))))
(define (tag->term tag)
  (if (oid? tag) (get-dterm tag (get-language))
      (if (tag? tag) (tag-term tag)
	  (if (string? tag)
	      (if (and (string? tag) (has-prefix tag "@")
		       (position #\" tag))
		  (string->lisp (subseq tag (position #\" tag)))
		  tag)
	      (fail)))))

(define (->tag tag (tagslot 'term))
  (if (oid? tag) (cons-tag (get-dterm tag (get-language)) tag)
      (if (tag? tag) tag
	  (if (string? tag)
	      (if (empty-string? tag) (fail)
		  (if (has-prefix tag "@")
		      (string->tag tag)
		      (cons-tag (difference (stdspace tag) "")
				{})))
	      (if (pair? tag)
		  (if (and (string? (car tag))  (oid? (cdr tag)))
		      (cons-tag (car tag) (cdr tag))
		      (if (and (string? (cdr tag))  (oid? (car tag)))
			  (cons-tag (cdr tag) (car tag))
			  {}))
		  (if (and (table? tag) (test tag tagslot) (get tag 'oid))
		      (cons-tag (get tag '{term tag}) (get tag 'oid))
		      {}))))))

(module-export!
 '{tag?
   cons-tag tag-oid tag-term
   ->tag tag->term tag->oid 
   tag->string string->tag})

;; We use the 'name' syntax of OIDs to have string representations which
;;  embed term information.  In particular @hi/lo"term" describes a tag
;;  with the string 'term' and the OID 'hi/lo'.
(define (tag->string tag (user (getuser)))
  (if (string? tag) tag
      (if (oid? tag)
	  (stringout (string-subst (get-dterm oid @?en) "@" "\\@")
		     "@" brico)
	  (if (tag? tag)
	      (let ((term (tag-term tag))
		    (oid (tag-oid tag)))
		(if (exists? oid)
		    (oid->string oid term)
		    term))))))

(define tagpat
  `#((opt (label oid #("@" (isxdigit+) "/" (isxdigit+)) #t))
     (label term (rest) #t)))

(define (string->tag string (user (getuser)))
  (let* ((match (text->frame tagpat string)))
    (cons-tag (get match 'term) (get match 'oid))))
