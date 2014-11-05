;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

(in-module 'mimeout)

(use-module '{texttools varconfig})
(define %used_modules 'varconfig)

(define (mimeout/simple headers content)
  (printout "MIME-Version: 1.0\r\n")
  (do-choices (key (getkeys headers))
    (when (or (string? key) (symbol? key))
      (let ((v (get headers key)))
	(when (and (singleton? v)
		   (or (number? v) (string? v) (timestamp? v)))
	  (printout
	    (if (or (and (string? key) (ascii? key))
		    (and (symbol? key) (ascii? (symbol->string key))))
		key
		(error "Can't handle non-ascii headers yet"))
	    ": "
	    (cond ((string? v)
		   (if (ascii? v)
		       (string-subst v "\n" "\n ")
		       (error "Can't handle non-ascii headers yet")))
		  ((number? v) v)
		  ((timestamp? v) (get v 'rfc822))
		  (else ""))
	    "\r\n")))))
  (if (packet? content)
      (let ((b64 (packet->base64 content)))
	(printout "Content-Transfer-Encoding: base64\r\n\r\n"
	  b64))
      (printout "Content-Transfer-Encoding: binary\r\n"
	"Content-length: " (byte-length content) "\r\n\r\n"
	content)))


