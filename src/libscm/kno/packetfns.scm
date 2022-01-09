;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2020 beingmeta, inc.  All rights reserved.
;;; Copyright (C) 2020-2022 Kenneth Haase (ken.haase@alum.mit.edu).

(in-module 'kno/packetfns)

(module-export!
 '{packet->digits digits->packet number->digits digits->number
   packetfns/base62 packetfns/base57 packetfns/base36
   packetfns/base26 packetfns/base32 packetfns/base16 packetfns/base10
   packet/condense}) 

(define packetfns/base62
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz")
(define packetfns/base57
  "ABCDEFGHJKLMNPQRSTUVWXYZ23456789abcdefghijkmnopqrstuvwxyz")
(define packetfns/base36
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
(define packetfns/base26
  "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
(define packetfns/base32
  "0123456789ABCDEFGHJKLMNPQRTVWXYZ")
(define packetfns/base16 "0123456789ABCDEF")
(define packetfns/base10 "0123456789")

(define (packet->digits packet (digits packetfns/base62))
  (let ((num (if (number? packet) packet
		 (string->number (packet->base16 packet) 16)))
	(base (length digits))
	(result '()))
    (while (>= num base)
      (set! result (cons (elt digits (remainder num base)) result))
      (set! num (quotient num base)))
    (set! result (cons (elt digits num) result))
    (apply string result)))

(define (digits->packet string (digits packetfns/base62))
  (let ((num 0) (base (length digits)) (i 0) (n (length string)))
    (dotimes (i n)
      (set! num (+ (* base num) (position (elt string i) digits))))
    (let* ((hex (number->string num 16))
	   (packet (if (zero? (remainder (length hex) 2))
		       (base16->packet hex)
		       (base16->packet (glom "0" hex)))))
      packet)))

(define (digits->number string (digits packetfns/base62))
  (let ((num 0) (base (length digits)) (i 0) (n (length string)))
    (dotimes (i n)
      (set! num (+ (* base num) (position (elt string i) digits))))
    num))

(define (packet/condense packet factor)
  (let* ((len (length packet))
	 (input (->vector packet)) 
	 (i (if (<= factor len) factor (remainder len factor)))
	 (lim (- len i))
	 (output (if (= i 0) '()
		     `(, (remainder (apply * (->list (map 1+ (slice input 0 i))))
				    256)))))
    (while (< i lim)
      (set! output (cons (remainder (apply * (->list (map 1+ (slice input i (+ i 4)))))
				    256)
			 output))
      (set! i (+ i factor)))
    (->packet (reverse output))))



