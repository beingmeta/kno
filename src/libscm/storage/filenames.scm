;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/filenames)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/filenames 
  " has been deprecated, please use " 'flexdb/filenames " instead")

(export-alias! 'flexdb/filenames)
