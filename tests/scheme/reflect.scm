;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(use-module '{reflection texttools ezrecords bench/miscfns})

(applytest 'reflection procedure-module procedure-module)
(applytest 'scheme procedure-module *)
(applytest 'texttools procedure-module textsubst)
(applytest 'bench/miscfns procedure-module fibi)
(applytest 'scheme procedure-module if)
(applytest 'ezrecords procedure-module defrecord)
