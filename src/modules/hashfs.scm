;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'hashfs)

;;; Virtual file system implemented on top of hashtables

(use-module '{mimetable ezrecords gpath})

(module-export! '{hashfs? hashfs/open hashfs/save!
		  hashfs/get hashfs/get+ hashfs/commit!})

(defrecord (hashfs OPAQUE)
  files (label (getuuid)) (source #f))

(define (hashfs/open (label #f) (init (make-hashtable)))
  (when (or (hashtable? label) (gpath? label))
    (set! init label) (set! label #f))
  (when (not label) (set! label (getuuid)))
  (cons-hashfs (if (not init) (make-hashtable) (hashfs-init init))
	       label (and (gpath? init) init)))

(define (hashfs-init init)
  (if (hashtable? init) init
      (if (gpath? init)
	  (if (gp/exists? init)
	      (let ((value (gp/fetch init)))
		(if (and value (exists? value) (packet? value))
		    (let ((hs (packet->dtype (gp/fetch init))))
		      (if (hashtable? hs) hs
			  (error "Invalid HASHFS init" init)))
		    (packet->dtype (gp/fetch init))))
	      (make-hashtable))
	  (error "Invalid HASHFS init" init))))

(define (hashfs/save! hashfs path data (type))
  (default! type (path->mimetype
		  path (if (packet? data) "application" "text")))
  (if (has-prefix path "/") (set! path (slice path 1)))
  (store! (hashfs-files hashfs) path
	  `#[data ,data ctype ,type modified ,(gmtimestamp)]))

(define (hashfs/get hashfs path)
  (if (has-prefix path "/") (set! path (slice path 1)))
  (get (get (hashfs-files hashfs) path) 'data))
(define (hashfs/get+ hashfs path)
  (if (has-prefix path "/") (set! path (slice path 1)))
  (get (hashfs-files hashfs) path))

(define (hashfs/commit! hashfs)
  (if (hashfs-source hashfs)
      (gp/save! (hashfs-source hashfs)
	(dtype->packet (hashfs-files hashfs)))
      (error "This HASHFS doesn't have a source" hashfs)))





