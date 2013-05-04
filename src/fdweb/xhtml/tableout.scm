;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'xhtml/tableout)

(use-module '{fdweb xhtml})

(module-export! 'tableout)

(define (tableout arg
		  (opts '()) (class)
		  (skipempty) (slotfns)
		  (maxdata))
  (if (string? opts) (set! opts `#[class ,opts]))
  (default! class (getopt opts 'class "fdjtdata"))
  (default! skipempty (getopt opts 'skipempty #f))
  (default! slotfns (getopt opts 'slotfns #f))
  (default! maxdata (getopt opts 'maxdata 1024))
  (let ((keys (getkeys arg)))
    (table* ((class class))
      (do-choices (key keys)
	(let ((values (get arg key)))
	  (cond ((and slotfns (test slotfns key))
		 ((get slotfns key) arg key (qc values)))
		((not (bound? values))
		 (unless skipempty
		   (tr (th key) (td* ((class "empty")) "Oddly empty value"))))
		((empty? values)
		 (unless skipempty
		   (tr (th key) (td* ((class "empty")) "No values"))))
		(else (do-choices (value values i)
			(if (zero? i)
			    (tr (th* ((rowspan (choice-size values)))
				  key)
				(td (xmlout value)))
			    (tr (td (xmlout value))))))))))))







