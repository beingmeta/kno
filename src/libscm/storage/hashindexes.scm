;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/hashindexes)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/hashindexes 
  " has been deprecated, please use " 'flexdb/hashindexes " instead")

(export-alias! 'flexdb/hashindexes)
