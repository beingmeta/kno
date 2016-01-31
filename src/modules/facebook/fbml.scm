;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'facebook/fbml)

;;; Outputting FBML (FaceBook Markup Language)

(use-module '{fdweb xhtml})

(define (fb:name (id #f))
  (if id (xmlelt "fb:name" 'uid (stringout id))
       (xmlelt "fb:name")))

(module-export! 'fb:name)

