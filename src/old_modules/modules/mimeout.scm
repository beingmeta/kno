;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'mimeout)

(use-module '{texttools mimetable packetfns crypto varconfig})
(define %used_modules 'varconfig)

(module-export! '{mimeout mimeout/simple mimeout/basic})

(define *always-exclude*
  '{include exclude multipart content-type content})

(define (mimeout/simple headers (content) (ctype))
  (default! content (try (get headers 'content) ""))
  (default! ctype 
    (try (get headers 'content-type)
	 (if (packet? content) "application/octet"
	     (if (string? content)
		 (if (textsearch #((bos) (spaces*) "<") content)
		     "text/html; charset=utf-8"
		     "text/plain; charset=utf-8")
		 (error "Invalid content")))))
  (printout "MIME-Version: 1.0\r\n")
  (mimeout/basic headers content ctype))

(define (mimeout/basic headers (content) (ctype))
  (default! content (try (get headers 'content) ""))
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
      (let* ((b64 (packet->base64 content))
	     (len (length b64))
	     (lines (quotient len 40))
	     (encoded
	      (stringout
		(dotimes (i lines)
		  (printout (slice b64 (* i 40) (* (1+ i) 40)) "\r\n"))
		(when (> len (* lines 40))
		  (printout (slice b64 (* lines 40)) "\r\n")))))
	(printout "Content-Transfer-Encoding: base64\r\n"
	  "Content-length: " (length encoded) "\r\n\r\n"
	  encoded))
      (begin
	(if (ascii? content)
	    (printout "Content-Transfer-Encoding: 7bit\r\n")
	    (printout "Content-Transfer-Encoding: 8bit\r\n"))
	(printout
	  "Content-length: " (byte-length content) "\r\n\r\n")
	(printout content "\r\n"))))

(defambda (output-headers headers)
  (when (test headers '%ordered)
    (doseq (key (get headers '%ordered))
      (output-header key (get headers key))))
  (do-choices (key (difference (getkeys headers)
			       (elts (get headers '%ordered))
			       (get headers '%ignore)
			       '{%ordered %ignore content-type content}))
    (when (or (string? key) (symbol? key))
      (output-header key (get headers key)))))
(defambda (output-header key v)
  (when (and (or (string? key) (symbol? key))
	     (singleton? v)
	     (or (number? v) (string? v) (timestamp? v)))
    (printout
      (if (or (and (string? key) (ascii? key))
	      (and (symbol? key) (ascii? (symbol->string key))))
	  (capitalize (->string key))
	  (error "Can't handle non-ascii headers yet"))
      ": "
      (cond ((string? v)
	     (if (ascii? v)
		 (string-subst v "\r\n" "\r\n ")
		 (error "Can't handle non-ascii headers yet")))
	    ((number? v) v)
	    ((timestamp? v) (get v 'rfc822))
	    (else ""))
      "\r\n")))

(defambda (mimeout headers (content))
  (default! content (try (get headers 'content) ""))
  (if (and (sequence? content) 
	   (not (or (string? content) (packet? content))))
      (let* ((packet (random-packet 16))
	     (frontier
	      (glom "____________________"
		(downcase (packet->digits packet packetfns/base36))))
	     (multipart (try (get headers 'multipart) "mixed"))
	     (len-1 (-1+ (length content))))
	(printout "MIME-Version: 1.0\r\n")
	(output-headers headers)
	(printout "Content-type: multipart/" (downcase multipart) 
	  "; boundary=\"" frontier "\"\r\n\r\n")
	(doseq (elt content i)
	  (if (pair? elt)
	      (if (string? (car elt))
		  (mimeout/simple `#[content-type ,(car elt)]
				  (cdr elt))
		  (mimeout/simple (car elt) (cdr elt)))
	      (if (table? elt)
		  (mimeout/basic elt (try (get elt 'content)
					  (get elt 'text)
					  ""))
		  (if (string? elt)
		      (mimeout/basic #[] elt)
		      (error |TypeError| "Invalid MIME element " elt))))
	  (if (= i len-1)
	      (printout "--" frontier "--\r\n")
	      (printout "--" frontier "\r\n"))))
      (mimeout/simple headers content)))

