;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/adjuncts)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/adjuncts 
  " has been deprecated, please use " 'flexdb/adjuncts " instead")

(export-alias! 'flexdb/adjuncts)
