;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/flexpools)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/flexpools 
  " has been deprecated, please use " 'flexdb/flexpools " instead")

(export-alias! 'flexdb/flexpool)
