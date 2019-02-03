;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/flexindexes)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/flexindexes 
  " has been deprecated, please use " 'flexdb/flexindexes " instead")

(export-alias! 'flexdb/flexindex)
