;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/xtags)

(use-module '{reflection texttools})
(use-module '{xtags}) ;; brico brico/dterms
(use-module '{fdweb xhtml xhtml/clickit i18n xhtml/brico xhtml/beingmeta})

(define small-oid-icon
  (stringout bmstatic "graphics/diamond12.png"))
(define checked-oid-icon
  (stringout bmstatic "graphics/diamond16.png"))
(define unchecked-oid-icon
  (stringout bmstatic "graphics/diamondhole16.png"))

(defambda (output-tags tags (var #f) (checked #f))
  (let ((tags (->tag tags))
	(checked (and var (or checked (req/get var {}))))
	(oidmap (make-hashtable))
	(terms {}))
    (do-choices (tag tags)
      (unless (test oidmap (tag-term tag)) (set+! terms (tag-term tag)))
      (add! oidmap (tag-term tag) (tag-oid tag)))
    (doseq (string (append (lexsorted (intersection terms checked))
			   (lexsorted (difference terms checked)))
		   i)
      (when (> i 0) (xmlout " \&middot; "))
      (span ((class (if var "tagspan checkspan" "tagspan")))
	(when var
	  (input TYPE "CHECKBOX"
		 NAME (if (symbol? var) (symbol->string var) var)
		 VALUE string
		 ("CHECKED" (if (overlaps? checked string) "CHECKED"))))
	(span ((class "term")) string)
	(do-choices (oid (get oidmap string))
	  (if var
	      (let ((ischecked (or (overlaps? oid checked)
				   (not (overlaps? string checked))))
		    (gloss (get-single-gloss oid (get-language))))
		(span ((class "checkspan")
		       (title (stringout "click to select/correct: " gloss))
		       (ischecked (if ischecked "yes")))
		  (input TYPE "CHECKBOX"
			 NAME (if (symbol? var) (symbol->string var) var)
			 VALUE (oid->string oid string)
			 ("CHECKED" ischecked))
		  (img SRC checked-oid-icon ALT "+" class "checked")
		  (img SRC unchecked-oid-icon ALT "o" class "unchecked")))
	      (img SRC small-oid-icon ALT "+"
		   TITLE (get-single-gloss oid default-language))))))))

(define (deeptag/span term oidlist)
  (span ((class "deeptag") )
    (span ((class "term")) term)
    (dolist (oid oidlist))))

(define (deeptag/checkbox var term oidlist (checked {}))
  (span ((class "deeptag checkspan"))
    (span ((class "term"))
      (input TYPE "CHECKBOX" NAME var VALUE term
	     ("CHECKED" (if (or (overlaps? #t checked)
				(overlaps? term checked))
			    "CHECKED")))
      term)
    (dolist (oid oidlist)
      (span ((class "checkspan")
	     (ischecked (if (or (overlaps? #t checked) (overlaps? oid checked))
			    "yes")))
	(input TYPE "CHECKBOX" NAME var VALUE (oid->string oid term)
	       ("CHECKED" (if (or (overlaps? #t checked)
				  (overlaps? term checked))
			      "CHECKED")))
	(img SRC oid-icon ALT "+"
	     TITLE (get-single-gloss oid default-language))))))

(define (deeptags/edit var taglist checked)
  (let ((strings '())
	(oidmap (make-hashtable)))
    (doseq (tag taglist)
      (let ((string (tag->string tag)) (oid (tag->oid tag))))
      (if (test oidmap string)
	  (add! oidmap string oid)
	  (begin (set! strings (cons string strings))
		 (add! oidmap string oid))))
    (doseq (string (reverse strings) i)
      (if (> i 0) (xmlout " "))
      (deeptag/checkbox
       var string
       (append (sorted (intersection checked (get oidmap string)))
	       (sorted (intersection (get oidmap string) checked)))
       (qc checked)))))

(module-export! '{output-tags deeptag/span deeptag/checkbox})
