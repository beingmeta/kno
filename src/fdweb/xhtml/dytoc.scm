;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/dytoc)

(module-export!
 '{with-dytoc
   dytoc/ref dytoc/div
   dytoc/sectnum dytoc/sectref dytoc/sectid})

(define with-dytoc
  (macro expr
    `(div ((class "with_dytoc")
	   (onmouseover "dytoc_display_sectid(event);")
	   (onmouseout "dytoc_undisplay_sectid(event);")
	   (onclick "dytoc_display_default(event);")
	   ,@(get-arg expr 1))
       ,@(cdr (cdr expr)))))

(defslambda (dytoc/sectnum table object)
  (try (get table object)
       (let ((count (try (get table '%count) 1)))
	 (store! table '%count (1+ count))
	 (store! table object count)
	 count)))
(define (dytoc/sectid table object)
  (let ((prefix (try (get table '%prefix) "SECT"))
	(num (dytoc/sectnum table object )))
    (string-append prefix num)))
(define (dytoc/sectref table object)
  (let ((prefix (try (get table '%prefix) "SECT"))
	(num (dytoc/sectnum table object )))
    (string-append prefix "REF" num)))

(define dytoc/ref
  (macro expr
    `(let* ((dytoc/table ,(get-arg expr 1))
	    (dytoc/obj ,(get-arg expr 2))
	    (dytoc/prefix (try (get dytoc/table '%prefix) "SECT"))
	    (dytoc/num (,dytoc/sectnum dytoc/table dytoc/object )))
       (span ((class "dytoc_sectref")
	      (name (string-append dytoc/prefix "REF" dytoc/num))
	      (sectid (string-append dytoc/prefix dytoc/num)))
	 ,@(subseq expr 3)))))

(define dytoc/div
  (macro expr
    `(let* ((dytoc/table ,(get-arg expr 1))
	    (dytoc/obj ,(get-arg expr 2))
	    (dytoc/prefix (try (get dytoc/table '%prefix) "SECT"))
	    (dytoc/num (,dytoc/sectnum dytoc/table dytoc/object )))
       (div ((class "dytoc_section")
	     (id (string-append dytoc/prefix dytoc/num)))
	 ,@(subseq expr 3)))))

