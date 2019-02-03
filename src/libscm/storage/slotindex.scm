;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/slotindex)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/slotindex 
  " has been deprecated, please use " 'flexdb/slotindex " instead")

(export-alias! 'flexdb/slotindex)
