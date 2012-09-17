;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2012 beingmeta, inc. All rights reserved

(in-module 'xhtml/signature)

(module-export! '{sig/make sig/check})

(define sigmod (%watch (get-module 'signature)))

(define sig/make (get sigmod 'sig/make))
(define sig/check (get sigmod 'sig/check))

