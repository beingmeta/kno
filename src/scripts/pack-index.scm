;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packindex})

(when (config 'optimized #t) (optimize*! 'knodb/actions/packindex))

