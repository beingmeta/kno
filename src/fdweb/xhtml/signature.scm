;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-

;;; Copyright (C) 2005-2017 beingmeta, inc. All rights reserved

(in-module 'xhtml/signature)

(module-export! '{sig/make sig/check})

(define sigmod (get-module 'signature))

(define sig/make (get sigmod 'sig/make))
(define sig/check (get sigmod 'sig/check))

