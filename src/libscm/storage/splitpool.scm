;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/splitpool)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/splitpool 
  " has been deprecated, please use " 'flexdb/splitpool " instead")

(export-alias! 'flexdb/splitpool)
