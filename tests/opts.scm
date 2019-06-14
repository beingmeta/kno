;;; -*- Mode: Scheme; text-encoding: latin-1 -*-

(load-component "common.scm")

(define opts1 #[x "three" y "four" z (five)])
(define opts2 #[x "nine" a "apple" z {}])
(define opts12 (list opts1 opts2))
(define opts21 (list opts2 opts1))

(evaltest '{a b c d e} (tryopt opts1 'q '{a b c d e}))
(evaltest "three" (tryopt opts1 'x '{a b c d e}))

(evaltest "three" (getopt opts12 'x #f))
(evaltest "nine" (getopt opts21 'x #f))
