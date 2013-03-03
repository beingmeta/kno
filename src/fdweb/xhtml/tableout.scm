;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved

(in-module 'xhtml/tableout)

(use-module '{fdweb xhtml})

(module-export! 'tableout)

(define (tableout arg (class "fdjtdata") (skipempty #f) (custom #f))
  (let ((keys (getkeys arg)))
    (table* ((class "fdjtdata"))
      (do-choices (key keys)
	(let ((values (get arg key)))
	  (cond ((and custom (test custom key))
		 ((get custom key) arg key (qc values)))
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







