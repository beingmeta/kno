;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/aggregates)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/aggregates 
  " has been deprecated, please use " 'flexdb/aggregates " instead")

(export-alias! 'flexdb/branches)
