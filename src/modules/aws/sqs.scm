;;; -*- Mode: Scheme; character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2013 beingmeta, inc.  All rights reserved.

(in-module 'aws/sqs)

(use-module '{aws aws/aws4 fdweb texttools logger varconfig})

(module-export! '{sqs/get sqs/send sqs/list sqs/delete sqs/extend})

(define-init %loglevel %info!)
;;(define %loglevel %debug!)

(define sqs-endpoint "https://sqs.us-east-1.amazonaws.com/")
(varconfig! sqs:endpoint sqs-endpoint)

(define (sqs-field-pattern name)
  `#(,(glom "<" name ">")
     (label body ,(parse-arg name) (not> ,(glom "</" name ">")))
     (glom "</" name ">")))

(define sqs-fields
  {#("<Body>" (label body (not> "</Body>")) "</Body>")
   #("<ReceiptHandle>" (label handle (not> "</ReceiptHandle>")) "</ReceiptHandle>")
   #("<MessageId>" (label msgid (not> "</MessageId>")) "</MessageId>")
   #("<RequestId>" (label reqid (not> "</RequestId>")) "</RequestId>")
   #("<QueueUrl>" (label queue (not> "</QueueUrl>")) "</QueueUrl>")
   #("<SenderId>" (label sent (not> "</SenderId>") #t) "</SenderId>")
   #("<SentTimestamp>" (label sent (not> "</SentTimestamp>") #t) "</SentTimestamp>")
   #("<ApproximateReceiveCount>" (label count (not> "</ApproximateReceiveCount>") #t)
     "</ApproximateReceiveCount>")
   #("<ApproximateReceiveTimestamp>" (label first (not> "</ApproximateReceiveTimestamp>") #t)
     "</ApproximateReceiveTimestamp>")})

(define (handle-sqs-response result)
  (debug%watch result)
  (if (<= 200 (getopt result 'response) 299)
      (let* ((combined (frame-create #f 'queue (getopt result '%queue)))
	     (content (getopt result '%content))
	     (fields (tryif content (text->frames sqs-fields content))))
	(do-choices (field fields)
	  (do-choices (key (getkeys field))
	    (add! combined key (get field key))))
	(if (exists? fields) combined (fail)))
      (error |Bad SQS response| sqs/get (get result 'effective-url)
	     result)))

(define (sqs/get queue)
  (handle-sqs-response
   (aws4/get (frame-create #f '%queue queue) queue
	     #["Action" "ReceiveMessage"])))

(define (sqs/send queue msg)
  (handle-sqs-response
   (aws4/get (frame-create #f '%queue queue) queue
	     `#["Action" "SendMessage" "MessageBody" ,msg])))

(define (sqs/list)
  (handle-sqs-response
   (aws4/get (frame-create #f) sqs-endpoint
	     #["Action" "ListQueues"])))

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




