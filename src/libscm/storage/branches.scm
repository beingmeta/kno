;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/branches)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/branches 
  " has been deprecated, please use " 'flexdb/branches " instead")

(export-alias! 'flexdb/branches)
