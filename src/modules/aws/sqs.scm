;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'aws/sqs)

(use-module '{aws aws/aws4 fdweb texttools logger varconfig})

(module-export! '{sqs/get sqs/send sqs/list sqs/delete sqs/extend})

(define-init %loglevel %info!)
;;(define %loglevel %debug!)

(define sqs-endpoint "https://sqs.us-east-1.amazonaws.com/")
(varconfig! sqs:endpoint sqs-endpoint)

(define (sqs-field-pattern name (label))
  (default! label (string->lisp name))
  `#(,(glom "<" name ">")
     (label ,label (not> ,(glom "</" name ">")))
     ,(glom "</" name ">")))

(define sqs-fields
  {(sqs-field-pattern "Body")
   (sqs-field-pattern "ReceiptHandle" 'handle)
   (sqs-field-pattern "MessageId" 'msgid)
   (sqs-field-pattern "RequestId" 'reqid)
   (sqs-field-pattern "QueueUrl" 'qurl)
   (sqs-field-pattern "SenderId" 'sender)
   (sqs-field-pattern "SentTimestamp" 'sent)
   (sqs-field-pattern "ApproximateReceiveCount")
   (sqs-field-pattern "ApproximateFirstReceiveTimestamp")})

(define (handle-sqs-response result)
  (debug%watch result)
  (if (<= 200 (getopt result 'response) 299)
      (let* ((combined (frame-create #f 'queue (getopt result '%queue {}) 'received (gmtimestamp)))
	     (content (getopt result '%content))
	     (fields (tryif content (debug%watch (text->frames sqs-fields content)
				      sqs-fields content))))
	(do-choices (field fields)
	  (do-choices (key (getkeys field))
	    (add! combined key (get field key))))
	(if (exists? fields) combined (fail)))
      (error |Bad SQS response| sqs/get (get result 'effective-url)
	     result)))

(define (sqs/get queue (opts #[]) (args `#["Action" "ReceiveMessage" "AttributeName.1" "all"]))
  (when (getopt opts 'wait) (store! args "WaitTimeSeconds" (getopt opts 'wait)))
  (when (getopt opts 'reserve) (store! args "VisibilityTimeout" (getopt opts 'reserve)))
  (handle-sqs-response (aws4/get (frame-create #f '%queue queue) queue args)))

(define (sqs/send queue msg (opts #[]) (args `#["Action" "SendMessage"]))
  (store! args "MessageBody" msg)
  (when (getopt opts 'delay) (store! args "DelaySeconds" (getopt opts 'delay)))
  (handle-sqs-response (aws4/get (frame-create #f '%queue queue) queue args)))

(define (sqs/list (prefix #f) (args #["Action" "ListQueues"]))
  (when prefix (set! args `#["Action" "ListQueues" "QueueNamePrefix" ,prefix]))
  (handle-sqs-response (aws4/get (frame-create #f) sqs-endpoint args)))

(define (sqs/delete message)
  (handle-sqs-response
   (aws4/get (frame-create #f '%queue (get message 'queue)) (get message 'queue)
	     `#["Action" "DeleteMessage" "ReceiptHandle" ,(get message 'handle)])))

(define (sqs/extend message secs)
  (handle-sqs-response
   (aws4/get (frame-create #f '%queue (get message 'queue))
	     (get message 'queue)
	     `#["Action" "ChangeMessageVisibility"
		"ReceiptHandle" ,(get message 'handle)
		"VisibilityTimeout" ,secs])))

