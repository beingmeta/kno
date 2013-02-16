;; -*- Coding: utf-8 -*-

;; English Morphology
;;  Copyright (C) 2001 Kenneth Haase, All Rights Reserved
;;  Copyright (C) 2001-2013 beingmeta, inc.

(in-module 'morph/freqs)

(use-module 'texttools)

(module-export!
 '{english-word-freqs english-root-freqs
   english-word-relfreqs english-root-relfreqs
   dataroot})

(define dataroot (get-component "data/"))

(define english-word-freqs
  (zipfile->dtype (get-component "data/en-word-freq.ztable")))
(define english-root-freqs
  (zipfile->dtype (get-component "data/en-root-freq.ztable")))
(define english-word-relfreqs
  (zipfile->dtype (get-component "data/en-word-relfreq.ztable")))
(define english-root-relfreqs
  (zipfile->dtype (get-component "data/en-root-relfreq.ztable")))




