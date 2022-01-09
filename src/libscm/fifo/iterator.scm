;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu)

(in-module 'fifo/iterator)

(use-module '{fifo})

(module-export! '{fifo->iterator})

(define (fifo->iterator fifo)
  (iterator () 
    [name (fifo-name fifo)] 
    (if (fifo/exhausted? fifo)
	#eod
	(fifo/pop fifo))))

