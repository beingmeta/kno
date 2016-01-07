;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2015 beingmeta, inc.  All rights reserved.

(in-module 'aws/ec2)

(use-module '{aws aws/v4 fdweb texttools mimetable regex logctl
	      ezrecords rulesets logger varconfig})
(define %used_modules '{aws varconfig ezrecords rulesets})

(module-export! '{ec2/opts})

(define ec2/key #f)
(varconfig! ec2:key ec2/key)
(define ec2/secret #f)
(varconfig! ec2:secret ec2/secret)

(define (ec2/opts (opts #[]))
  (frame-create #f
    'aws:key (getopt opts 'aws:key (or ec2/key aws/key))
    'aws:secret (getopt opts 'aws:secret (or ec2/key aws/secret))))

