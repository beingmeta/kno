;;; -*- Mode: scheme; text-encoding: utf-8; -*-

(load-component "common.scm")

(use-module '{crypto pem})

(config! 'hexpacket #f)

(define b64 base64->packet)

(define key8 #X"0efe80dfbbb8aa68")
(define key16 #X"5f30ed78063ba529f8635ab3a26a299a")
(define key24 #X"b3d3be58fcb6bb56695ec092f8e4992cdb7f7e306cee8979")
(define key32 
  #X"00a417997e71b5dd9f7f19d64e2c23bcbe31bb3f1076f7bade9ad2de1e5f081e")

(define iv8 #X"b7f488792cf0bd84")
(define ziv8 (fill-packet 8 0))
(define iv16 #X"3ea141e1fc673e017e97eadc6b968f38")
(define ziv16 (fill-packet 16 0))

(define sample "Keep it secret, keep it safe")
(define sample-packet (string->packet "Keep it secret, keep it safe"))
(define password "mellon")
;; openssl enc -e -a -bf -in sample -K `cat key16.hex` -iv `cat iv8.hex`
(define aes-encrypted-sample16
  #X@"u8H5dri2nNzUkqTO7QS2Ojf0qE5qxk90+ThFqbEhfoU=")
(define aes-encrypted-sample32
  #X@"I5t/w/QNf2RmAK/0w2OnKgIhzWKocD0hngSJhZDz82k=")

(define bf-encrypted-sample16
  #X@"33LmggVnMlXFnYMC/phQaOItzP7WD2H3kFswc9OXLT8=")
;; Note that the bigger key examples can't be compared with the
;; openssl command line because the command line can't handle big hex
;; numbers

(define random-input (random-packet 2048))

;; openssl enc -rc4 -in sample -K `cat key16.hex` -a
(applytest #X@"T9OM7k4y8+mlD8CASAVVqkKpE6pxJgCNMHBFIg=="
	   encrypt sample key16 "RC4")

(applytest aes-encrypted-sample16 encrypt sample key16 "AES128" iv16)
(applytest sample decrypt->string aes-encrypted-sample16 key16 "AES128" iv16)

(applytest aes-encrypted-sample32 encrypt sample key32 "AES256" iv16)
(applytest sample decrypt->string aes-encrypted-sample32 key32 "AES256" iv16)

(applytest bf-encrypted-sample16 encrypt sample key16 "BF" iv8)
(applytest sample decrypt->string bf-encrypted-sample16 key16 "BF" iv8)

(define (test-algorithm name (usekey key32) (iv (random-packet 8)))
  (when (and iv (number? iv)) (set! iv (random-packet iv)))
  (evaltest random-input 
	    (decrypt (encrypt random-input usekey name iv)
		     usekey name iv)))

(test-algorithm "RC4" key16)
(test-algorithm "CAST" key16)
(test-algorithm "DES" key8)
(test-algorithm "DES3" key24)
(test-algorithm "AES256" key32 16)
;; (test-algorithm "AES128" key16 16)

;;; RSA tests

(define rsa.pem (pem->packet (filestring (get-component "./data/crypto/rsa.pem"))))
(define rsa.pub (pem->packet (filestring (get-component "./data/crypto/rsa.pub"))))

(define sample.rsa (filedata (get-component "./data/crypto/sample.rsa")))
(define sample.rsapub (filedata (get-component "./data/crypto/sample.rsapub")))

(applytest #"foobar" decrypt
	   (filedata (get-component "./data/crypto/foobar.rsa"))
	   rsa.pub "RSAPUB")
(applytest sample-packet decrypt
	   (filedata (get-component "./data/crypto/sample.rsa"))
	   rsa.pem "RSA")
(applytest sample-packet decrypt
	   (filedata (get-component "./data/crypto/sample.rsapub"))
	   rsa.pem "RSA")
(applytest sample-packet decrypt (encrypt sample rsa.pub "RSAPUB")
	   rsa.pem "RSA")

(test-finished "CRYPTOTEST")
