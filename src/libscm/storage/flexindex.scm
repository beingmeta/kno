;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/flexindex)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/flexindex 
  " has been deprecated, please use " 'flexdb/flexindex " instead")

(export-alias! 'flexdb/flexindex)
