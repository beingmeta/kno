;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(define version "$Id:$")
(define revision "$Revision:$")

(config! 'bricosource "/data/bg/brico")
(config! 'cachelevel 2)
(use-module '{brico mttools})
(define all-slots (make-hashset))
(define (dotest)
  (do-choices-mt (f (pool-elts brico-pool) 4 8192 mttools/fetchoids)
     (hashset-add! all-slots (getslots f))))

