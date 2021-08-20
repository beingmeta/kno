;;; -*- Mode: Scheme; text-encoding: utf-8 -*-

(load-component "common.scm")

(use-module '{reflection io/binary varconfig text/stringfmts bench/randobj})

(define xtype-test-obj
  `(#[foo 3 bar 8] #"12\0645\14789\253cdef123\105\14789"
    #t #f #\a () 9739 -3 3.1415 #\u3f4e
    ,(vector 34 35 1024)
    ,(vector 34 35 1024 150000)
    ,(vector 3.4 3.5 1000000000.05 .00000001)
    ,(vector 3.4 3.5 100000000000.05 .000000000000000001)
    ,@'((test) "te \" \" st" "" test #() b c)))

;;; Testing XTypes

(evaltest xtype-test-obj (decode-xtype (encode-xtype xtype-test-obj)))

(define (make-xtype-test-objects n)
  (let ((results {}))
    (dotimes (i n)
      (set+! results (frame-create #f
		       'serial i
		       'alpha_slot (random-string) 
		       'beta_slot (random-string)
		       'gamma_slot (random 100000)
		       'delta_slot (random 100000)
		       'epsilon_slot (tryif (zero? (random 1)) (random-string)))))
    results))

(let* ((all (make-xtype-test-objects 42))
       (xtype1 (encode-xtype (qc all)))
       (xtype2 (write-xtype (qc all) #f))
       (xtype3 (precode-xtype (qc all) #f))
       (embed-opts [xrefs '{alpha_slot beta_slot gamma_slot delta_slot epsilon_slot phi_slot} embed #t])
       (extype1 (encode-xtype (qc all) embed-opts))
       (extype2 (write-xtype (qc all) #f embed-opts))
       (extype3 (precode-xtype (qc all) embed-opts)))
  (applytest #t equal? xtype1 xtype2)
  (applytest #t equal? extype1 extype2)
  (applytest #t < (length extype1) (length xtype1)))

