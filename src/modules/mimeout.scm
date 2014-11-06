;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2014 beingmeta, inc.  All rights reserved.

(in-module 'mimeout)

(use-module '{texttools mimetable packetfns crypto varconfig})
(define %used_modules 'varconfig)

(module-export! '{mimeout mimeout/simple})

(define *always-exclude* '{include exclude multipart content-type content})

(define (mimeout/simple headers content (ctype))
  (default! ctype 
    (try (get headers 'content-type)
	 (if (packet? content) "application/octet"
	     (if (string? content)
		 (if (textsearch #((bos) (spaces*) "<") content)
		     "text/html; charset=utf-8"
		     "text/plain; charset=utf-8")
		 (error "Invalid content")))))
  (output-headers headers)
  (printout "Content-type: " ctype "\r\n")
  (if (packet? content)
      (let ((b64 (packet->base64 content)))
	(printout "Content-Transfer-Encoding: base64\r\n\r\n"
	  b64))
      (begin
	(unless (ascii? content)
	  (printout "Content-Transfer-Encoding: binary\r\n"))
	(printout
	  "Content-length: " (byte-length content) "\r\n\r\n")
	(printout content))))

(defambda (output-headers headers (include {}) (except {}))
  (do-choices (key (difference (if (exists? include) 
				   (intersection (getkeys headers) include)
				   (getkeys headers))
			       except))
    (when (or (string? key) (symbol? key))
      (let ((v (get headers key)))
	(when (and (singleton? v)
		   (or (number? v) (string? v) (timestamp? v)))
	  (printout
	    (if (or (and (string? key) (ascii? key))
		    (and (symbol? key) (ascii? (symbol->string key))))
		(capitalize (->string key))
		(error "Can't handle non-ascii headers yet"))
	    ": "
	    (cond ((string? v)
		   (if (ascii? v)
		       (string-subst v "\n" "\n ")
		       (error "Can't handle non-ascii headers yet")))
		  ((number? v) v)
		  ((timestamp? v) (get v 'rfc822))
		  (else ""))
	    "\r\n"))))))

(defambda (mimeout headers content (include) (except))
  (default! include (get headers 'include))
  (default! except (choice (get headers 'exclude) 'content-type
			   *always-exclude*))
  (if (and (sequence? content) 
	   (not (or (string? content) (packet? content))))
      (let* ((packet (random-packet 16))
	     (frontier (downcase (packet->digits packet packetfns/base36)))
	     (multipart (try (get headers 'multipart) "mixed"))
	     (len-1 (-1+ (length content))))
	(printout "MIME-Version: 1.0\r\n")
	(output-headers headers {} except)
	(printout "Content-type: multipart/" (downcase multipart) 
	  "; boundary=" frontier "\r\n\r\n")
	(doseq (elt content i)
	  (if (pair? elt)
	      (mimeout/simple (car elt) (cdr elt))
	      (if (table? elt)
		  (mimeout/simple elt (try (get elt 'content) ""))
		  (mimeout/simple #[] elt)))
	  (if (= i len-1)
	      (printout "--" frontier "--\n")
	      (printout "--" frontier "\n"))))
      (mimeout/simple headers content)))








