;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/typeindex)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/typeindex 
  " has been deprecated, please use " 'flexdb/typeindex " instead")

(export-alias! 'flexdb/typeindex)
