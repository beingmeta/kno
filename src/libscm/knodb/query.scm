;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'knodb/query)

(use-module '{fifo engine text/stringfmts logger varconfig})
(use-module '{knodb knodb/fuzz knodb/fuzz/terms knodb/fuzz/strings})

(define-init %loglevel %notice%)
