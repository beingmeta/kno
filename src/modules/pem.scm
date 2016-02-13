;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'pem)

(use-module '{logger texttools})

(define-init %loglevel %warn%)

(module-export! '{readpem pem->packet pemout ->pem})

(define pem-pattern
  `(GREEDY
    #((bol) (label dashcount #("----" (+ "-")) ,length)
      "BEGIN " (label head (not> "----")) (+ "-") "\n"
      (label body (not> "-") ,base64->packet)
      (bol) "----" (+ "-") "END " (label foot (not> "----")) (+ "-") (eol))))

(define (readpem string)
  (text->frames pem-pattern string))

(define (pem->packet string)
  (get (text->frames pem-pattern string) 'body))

(define (pemout packet (header "PEM") (dashcount 5) (linelen 64))
  (let* ((base64 (packet->base64 packet))
	 (lines (quotient (length base64) linelen)))
    (lineout (make-string dashcount #\-) "BEGIN " header (make-string dashcount #\-))
    (dotimes (i lines)
      (lineout (slice base64 (* i linelen) (* (1+ i) linelen))))
    (lineout (slice base64 (* lines linelen)))
    (lineout (make-string dashcount #\-) "END " header (make-string dashcount #\-))))

(define (->pem packet (header "PEM") (dashcount 5))
  (stringout (pemout packet header dashcount)))


