;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.

(in-module 'knodb/flex)

(use-module '{knodb knodb/flexindex knodb/flexpool})

(module-export! '{flex/ref flex/make flex/partitions 
		  flex/container flex/container!
		  flex/commit! flex/save!})

(define flex/ref knodb/ref)
(define flex/make knodb/make)
(define flex/partitions knodb/partitions)
(define flex/mods knodb/mods)
(define flex/modified? knodb/modified?)
(define flex/commit! knodb/commit!)
(define flex/save! knodb/save!)

(define flex/open-index knodb/open-index)
