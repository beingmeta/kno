;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(use-module 'crypto)

(define key
  #"\d9f-\e3\b5\a6\c7\b3\b0\e2W\1f\d5\0e\14]\87@ME\c4K\d6\06\98:g\dc\c00\7f\99\96\ac|KRC\ff\02%V\22\fad6X\ebv\a50:\f1\07A\89")
(define key8 #"\0e\fe\80\df\bb\b8\aah")
(define key16 #"_0\edx\06;\a5)\f8cZ\b3\a2j)\9a")
(define key24 #"\b3\d3\beX\fc\b6\bbVi^\c0\92\f8\e4\99,\db\7f~0l\ee\89y")
(define key32 
  #"\00\a4\17\99~q\b5\dd\9f\7f\19\d6N,#\bc\be1\bb?\10v\f7\ba\de\9a\d2\de\1e_\08\1e")

(define iv8 #"\b7\f4\88y,\f0\bd\84")

(define sample "Keep it secret, keep it safe")
(define encrypted-sample
  #":\89\e9\a0\13\d3SFe\9eY\cb\92ApfX\95\bf\beq\cf\d3!^~\a3\8ap\1a\95>")
(define random-input (random-packet 2048))

(applytest encrypted-sample encrypt sample key "BF" iv8)
(applytest sample decrypt->string encrypted-sample key "BF" iv8)
(evaltest random-input (decrypt (encrypt random-input key "BF" iv8)
				key "BF" iv8))

(define (test-algorithm name (usekey key) (iv (random-packet 8)))
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

