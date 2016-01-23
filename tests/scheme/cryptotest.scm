;;; -*- Mode: scheme; text-encoding: utf-8; -*-
(load-component "common.scm")

(use-module 'crypto)

(config! 'hexpacket #f)

(define b64 base64->packet)

(define key8 #X"0efe80dfbbb8aa68")
(define key16 #X"5f30ed78063ba529f8635ab3a26a299a")
(define key24 #X"b3d3be58fcb6bb56695ec092f8e4992cdb7f7e306cee8979")
(define key32 
  #X"00a417997e71b5dd9f7f19d64e2c23bcbe31bb3f1076f7bade9ad2de1e5f081e")
(define key56
  #X"d9662de3b5a6c7b3b0e2571fd50e145d87404d45c44bd606983a67dcc0307f9996ac7c4b5243ff02255622fa643658eb76a5303af1074189")
(define key64
  #X"be6189f95cbba8990f95b1ebf1b305eff700e9a13ae5ca0bcbd0484764bd1f231ea81c7b64c514735ac55e4b79633b706424119e09dcaad4acf21b10af3b33cd")

(define iv8 #"\b7\f4\88y,\f0\bd\84")
(define ziv8 (fill-packet 8 0))

(define sample "Keep it secret, keep it safe")
(define password "mellon")
(define encrypted-sample64
  #X@"+jE94JNmvNXQvILE2jsfnEOushT0UNKIVc4th2NHHbA=")
(define encrypted-sample56
  #X@"OonpoBPTU0ZlnlnLkkFwZliVv75xz9MhXn6jinAalT4=")
;; openssl enc -e -a -bf -in sample -K `cat key16.hex` -iv `cat iv8.hex`
(define encrypted-sample16
  #X@"33LmggVnMlXFnYMC/phQaOItzP7WD2H3kFswc9OXLT8=")
;; Note that the bigger key examples can't be compared with the
;; openssl command line because the command line can't handle big hex
;; numbers

(define random-input (random-packet 2048))

;; openssl enc -rc4 -in sample -K `cat key16.hex` -a
(applytest #X@"T9OM7k4y8+mlD8CASAVVqkKpE6pxJgCNMHBFIg=="
	   encrypt sample key16 "RC4")

(applytest encrypted-sample64 encrypt sample key64 "BF" iv8)
(applytest encrypted-sample56 encrypt sample key56 "BF" iv8)
(applytest encrypted-sample16 encrypt sample key16 "BF" iv8)
(applytest sample decrypt->string encrypted-sample64 key64 "BF" iv8)
(applytest sample decrypt->string encrypted-sample56 key56 "BF" iv8)
(applytest sample decrypt->string encrypted-sample16 key16 "BF" iv8)
(evaltest random-input (decrypt (encrypt random-input key56 "BF" iv8)
				key56 "BF" iv8))

(define (test-algorithm name (usekey key56) (iv (random-packet 8)))
  (when (and iv (number? iv)) (set! iv (random-packet iv)))
  (evaltest random-input 
	    (decrypt (encrypt random-input usekey name iv)
		     usekey name iv)))

(test-algorithm "RC4")
(test-algorithm "RC4" key8)
(test-algorithm "CAST" key16)
(test-algorithm "CAST" key32)
(test-algorithm "CAST")
(test-algorithm "DES" key8)
(test-algorithm "DES3" key24)
(test-algorithm "AES256" key32 16)
(test-algorithm "AES128" key16 16)

