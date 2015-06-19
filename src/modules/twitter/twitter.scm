;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'twitter)

(use-module '{fdweb texttools reflection varconfig logger})
(use-module '{xhtml xhtml/auth})

(define %loglevel %notice!)
;;(define %loglevel %debug!)

