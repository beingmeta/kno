;;; -*- Mode: Scheme -*-

(use-module '{optimize knodb/actions/packpool})

(when (config 'optimized #t) (optimize*! 'knodb/actions/packpool))

