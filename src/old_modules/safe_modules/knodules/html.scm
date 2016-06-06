;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'knodules/html)

;;; Tools for output of knodules and dterms in XHTML

(use-module '{texttools ezrecords varconfig fdweb xhtml xhtml/clickit})

(use-module 'knodules)

(define span/dterm
  (macro expr
    `(let* ((_ref ,(get-arg expr 1))
	    (_oid (if (oid? _ref) _ref (,kno/dterm _ref)))
	    (_dterm (if (string? _ref) _ref (get _oid 'dterm))))
       (span ((class "dterm") (dterm _dterm))
	 ,@(cddr expr)))))

(define (dtermfield dterm rel (edit #f))
  (let ((vals (get dterm rel))
	(relname (if (symbol? rel) (symbol->string rel)
		     (get rel 'dterm)))
	(relcode (try (get kno/relcodes rel) #f)))
    (when (or edit (exists? vals))
      (span ((class (if edit "field editfield" "field")))
	(span ((class "head")) "// " relname)
	(when edit (input TYPE "TEXT" NAME relname VALUE ""))
	(do-choices (v vals i)
	  (if (> i 0) (xmlout " ") (xmlout (nbsp)))
	  (if edit
	      (span ((class "checkspan"))
		(input TYPE "CHECKBOX" NAME relname
		       VALUE (if (oid? v) (get v 'dterm) v))
		(when relcode (span ((class "relcode")) relcode))
		(if (oid? v)
		    (span ((class "dterm") (dterm (get v 'dterm)))
		      (get v 'dterm))
		    (xmlout v)))
	      (begin (when relcode (span ((class "relcode")) relcode))
		     (if (oid? v)
			 (span ((class "dterm") (dterm (get v 'dterm)))
			   (get v 'dterm))
			 (xmlout v)))))))))

