;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/registry)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/registry 
  " has been deprecated, please use " 'flexdb/registry " instead")

(export-alias! 'flexdb/registry)
