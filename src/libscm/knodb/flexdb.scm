;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2019 beingmeta, inc.  All rights reserved.

(in-module 'knodb/flexdb)

(use-module 'knodb)

(module-export! '{flexdb/ref flexdb/make flexdb/partitions 
		  flexdb/container flexdb/container!
		  flexdb/commit! flexdb/save!})

(define flexdb/ref knodb/ref)
(define flexdb/make knodb/make)
(define flexdb/partitions knodb/partitions)
(define flexdb/mods knodb/mods)
(define flexdb/modified? knodb/modified?)
(define flexdb/commit! knodb/commit!)
(define flexdb/save! knodb/save!)
