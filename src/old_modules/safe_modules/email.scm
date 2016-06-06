;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc. All rights reserved

(in-module 'email)

(use-module '{fdweb texttools varconfig logger})

(module-export!
 '{email/pattern email/ok? email/std email/hash email/host email/name})

(define-init %loglevel %notify!)
(varconfig! sbooks:email:loglevel %loglevel)

;;; Email checking functions

(define email/pattern
  #((isalnum) (* {(isalnum) "." "-" "_" "+"})
    "@" (+ #((+ {(isalnum) "-"}) "."))
    (isalnum+) (eos)))

(define (email/ok? addr)
  (and (string? addr)
       (textmatch email/pattern (trim-spaces addr))))

(define (email/std addr)
  (if (and (string? addr) (textmatch email/pattern (trim-spaces addr)))
      (downcase (trim-spaces addr))
      (error 'bademail string)))

(define (email/hash addr (hashfn md5))
  (if (and (string? addr) (textmatch email/pattern (trim-spaces addr)))
      (downcase (packet->base16 (hashfn (downcase (trim-spaces addr)))))
      (error 'bademail string)))

(define (email/name string)
  (if (and (string? string) (textmatch email/pattern (trim-spaces string)))
      (if (position #\@ string)
	  (subseq string 0 (position #\@ string))
	  string)
      (error 'bademail string)))

(define (email/host string)
  (if (and (string? string) (textmatch email/pattern (trim-spaces string)))
      (if (position #\@ string)
	  (subseq string (1+ (position #\@ string)))
	  string)
      (error 'bademail string)))




