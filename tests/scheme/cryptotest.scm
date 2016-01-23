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

(define iv8 #"\b7\f4\88y,\f0\bd\84")
(define ziv8 (fill-packet 8 0))
(define iv16 #X"3ea141e1fc673e017e97eadc6b968f38")
(define ziv16 (fill-packet 16 0))

(define sample "Keep it secret, keep it safe")
(define password "mellon")
;; openssl enc -e -a -bf -in sample -K `cat key16.hex` -iv `cat iv8.hex`
(define aes-encrypted-sample16
  #X@"guMybKzrGs/zN2aAU8BDz4uCR/68Jh8s0r8KLOhH38U=")
(define bf-encrypted-sample16
  #X@"33LmggVnMlXFnYMC/phQaOItzP7WD2H3kFswc9OXLT8=")

;; Note that the bigger key examples can't be compared with the
;; openssl command line because the command line can't handle big hex
;; numbers
(define bf-encrypted-sample56
  #X@"OonpoBPTU0ZlnlnLkkFwZliVv75xz9MhXn6jinAalT4=")

(define random-input (random-packet 2048))

;; openssl enc -rc4 -in sample -K `cat key16.hex` -a
(applytest #X@"T9OM7k4y8+mlD8CASAVVqkKpE6pxJgCNMHBFIg=="
	   encrypt sample key16 "RC4")

(applytest aes-encrypted-sample16 encrypt sample key16 "AES" iv8)
(applytest sample decrypt->string aes-encrypted-sample16 key16 "AES" iv8)

(applytest bf-encrypted-sample16 encrypt sample key16 "BF" iv8)
(applytest sample decrypt->string bf-encrypted-sample16 key16 "BF" iv8)

(applytest bf-encrypted-sample56 encrypt sample key56 "BF" iv8)
(applytest sample decrypt->string bf-encrypted-sample56 key56 "BF" iv8)

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

