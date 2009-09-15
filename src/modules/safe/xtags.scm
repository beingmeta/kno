;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2006-2009 beingmeta, inc.  All rights reserved.

(in-module 'xtags)

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
  (if (oid? tag) (oid->dterm tag)
      (if (tag? tag) (tag-term tag)
	  (if (string? tag)
	      (if (and (string? tag) (has-prefix tag "@")
		       (position #\" tag))
		  (string->lisp (subseq tag (position #\" tag)))
		  tag)
	      (fail)))))

(define (->tag tag (tagslot 'term))
  (if (oid? tag) (cons-tag (oid->dterm tag) tag)
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

(define (get-language-info)
  (try (cgiget 'language)
       (threadget 'language)
       default-language))

(define (oid->dterm oid (language (get-language-info)))
  (if (test oid 'knowlet)
      (let* ((knowlet (try (knowlet (get oid 'knowlet))
			   (get oid 'knowlet)))
	     (try (get oid 'term) (get oid 'dterm)
		  (pick-one (get oid (tryif (knowlet? knowlet)
				       (knowlet-language knowlet))))
		  (pick-one (get oid (get language 'language)))))
	(if (oid? knowlet)
	    (stringout (string-subst (get oid 'term) "@" "\\@")
		       (oid->string knowlet))
	    (stringout (string-subst (get oid 'term) "@" "\\@")
		       "@" (knowlet-name knowlet))))
      (get-dterm oid language)))

(module-export!
 '{tag?
   cons-tag tag-oid tag-term
   ->tag tag->term tag->oid 
   tag->string string->tag})

;; We use the 'name' syntax of OIDs to have string representations which
;;  embed term information.  In particular @hi/lo"term" describes a tag
;;  with the string 'term' and the OID 'hi/lo'.
(define (tag->string tag)
  (if (string? tag) tag
      (if (oid? tag) (oid->dterm tag)
	  (if (tag? tag)
	      (let ((term (tag-term tag))
		    (oid (tag-oid tag)))
		(if (exists? oid)
		    (oid->string oid term)
		    term))))))

(define tagpat
  `(GREEDY #((opt (label oid #("@" (isxdigit+) "/" (isxdigit+)) #t))
	     (label term (rest) #t))))

(define (string->tag string)
  (let* ((match (text->frame tagpat string)))
    (cons-tag (get match 'term) (get match 'oid))))

(defambda (reduce-tags tags)
  "Removes OID tags whose term is not included and terms which have redundant
   entries with OIDs"
  (let* ((oided (pick tags tag-oid))
	 (unoided (difference tags oided))
	 (oidterms (tag-term oided))
	 (rawterms (tag-term unoided)))
    (choice (reject unoided tag-term oidterms)
	    (pick oided tag-term rawterms))))

(module-export! 'reduce-tags)
