;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'kno/testscript)

(module-export! '{main})

(use-module '{logger text/stringfmts})

(define %loglevel %info%)

(define (testscript . args)
  (lognotice |TestScript| "Listing " ($count (length args) "command arg"))
  (doseq (arg args i) (lineout " #" (1+ i) "\t" arg)))

(define main testscript)
