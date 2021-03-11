;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packindex})

(when (config 'optimized #t) (optimize-module! (get (get-module 'knodb/actions/packindex) '%optimize)))

