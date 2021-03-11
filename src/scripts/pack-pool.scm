;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packpool})

(when (config 'optimized #t) (optimize-module! (get (get-module 'knodb/actions/packpool) '%optimize)))

