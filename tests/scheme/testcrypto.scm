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

(define sample "Keep it secret, keep it safe")
(define encrypted-sample
  #"GR\fcHc\a3k\85\c7\dbI\f3$+\06\9b=\0a@\07\0c\e80\ea>\10\e2dg&\ad%")
(define random-input (random-packet 2048))

(applytest encrypted-sample encrypt sample key)
(applytest sample decrypt->string encrypted-sample key)
(evaltest random-input (decrypt (encrypt random-input key) key))

(define (test-algorithm name (usekey key))
  (evaltest random-input 
	    (decrypt (encrypt random-input usekey name)
		     usekey name)))

(test-algorithm "RC4")
(test-algorithm "RC4" key8)
(test-algorithm "DES" key8)
(test-algorithm "DES3" key24)
(test-algorithm "AES256" key32)
(test-algorithm "AES128" key16)
(test-algorithm "CAST" key16)
(test-algorithm "CAST" key32)
(test-algorithm "CAST")

