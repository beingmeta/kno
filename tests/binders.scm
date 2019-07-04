;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(errtest (let foo))
(errtest (let ((x 3)) . body))

