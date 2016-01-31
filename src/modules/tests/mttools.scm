;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'tests/mttools)

(use-module 'mttools)

(module-export! 'test-mttools)

(config! 'bricosource "/data/bg/brico")
(config! 'cachelevel 2)
(use-module '{brico mttools})
(define all-slots (make-hashset))

(define (test-mttools)
  (do-choices-mt (f (pool-elts brico-pool) 4 8192 mt/fetchoids)
     (hashset-add! all-slots (getslots f))))

