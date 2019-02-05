;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2010-2019 beingmeta, inc. All rights reserved

(in-module 'storage/flex)
(use-module 'logger)

(logwarn |Deprecated| 
  "The module " 'storage/flex 
  " has been deprecated, please use " 'flexdb " instead")

(irritant #t |InFlex|)

(export-alias! 'flexdb)
