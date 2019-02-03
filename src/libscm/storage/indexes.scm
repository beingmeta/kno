;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/indexes)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/indexes 
  " has been deprecated, please use " 'flexdb/indexes " instead")

(export-alias! 'flexdb/indexes)
