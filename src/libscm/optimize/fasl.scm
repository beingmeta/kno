;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'optimize/fasl)

(use-module '{texttools logger knodb/countrefs})

(module-export! '{faslize xrefs/faslize})

(define (faslize in (out))
  (default! out (glom in ".fasl"))
  (let* ((in (open-input-file in))
	 (expr (read in)))
    (until (eof-object? expr)
      (write-xtype expr out)
      (set! expr (read in)))
    (logwarn |Final| expr)
    (close out)))

(define (xrefs/faslize infile (outfile))
  (default! outfile (glom infile ".fasl"))
  (let* ((in (open-input-file infile))
	 (expr (read in))
	 (exprs '()))
    (until (eof-object? expr)
      (set! exprs (cons expr exprs))
      (set! expr (read in)))
    (let* ((freqs (countrefs exprs))
	   (out (open-byte-output outfile))
	   (ordered (reverse (->vector exprs)))
	   (refsvec (rsorted (hashtable-skim freqs 2) freqs))
	   (opts [xrefs (xtype/refs refsvec)]))
      (logwarn |XREFS| (length refsvec))
      (write-xtype (vector->compound refsvec '%fasl_xrefs) out)
      (doseq (expr ordered)
	(write-xtype expr out opts))
      (close out))))
