;;; -*- Mode: Scheme; Character-encoding: utf-8; -*-
;;; Copyright (C) 2005-2016 beingmeta, inc.  All rights reserved.

(in-module 'aws/associates)

;;; Access to Amazon Associates functions

(use-module '{fdweb texttools})

(module-export! '{aauri aaitems})

(define amazon-uri "http://ecs.amazonaws.com/onca/xml")
(define access-key-id "1QYJ5P9NZE42YRQK5B02")
(define associates-id "beingmeta-20")

(define (aauri . args)
  (apply scripturl amazon-uri
	 "Service" "AWSECommerceService"
	 "AWSAccessKeyId" access-key-id
	 "Version" "2008-03-03"
	 "AssociateTag" associates-id
	 args))

(define (aaget . args)
  (urlget (apply aauri args)))

(define (aagetxml . args)
  (urlxml (apply aauri args) 'data) )

(define (aagetitems index . args)
  (let* ((response
	  (xmlget (urlxml
		   (apply aauri
			  "Operation" "ItemSearch" "SearchIndex" index
			  args)
		   'data)
			   'itemsearchresponse))
	 (items (get response 'items)))
    (if (test (get items 'request) 'isvalid "True") 
	(get items 'item)
	(fail))))

(define (testattrib attribs slotid value)
  (or (test attribs slotid value)
      (and (string? value)
	   (or (string-ci=? value (get attribs slotid))
	       (textsearch `(IC ,value) (get attribs slotid))))))

(define (itemextract item)
  (modify-frame (get item 'itemattributes)
    'asin (get item 'asin) 'url (get item 'detailpageurl)))

(define (aaitems index field1 value1 (field2 #f) (value2 #f))
  (let ((items (if field2
		   (aagetitems index
			       (capitalize (symbol->string field1))
			       value1
			       (capitalize (symbol->string field2))
			       value2)
		   (aagetitems index
			       (capitalize (symbol->string field1))
			       value1))))
    (itemextract
     (filter-choices (item items)
       (and (testattrib (get item 'itemattributes) field1 value1)
	    (or (not field2)
		(testattrib (get item 'itemattributes) field2 value2)))))))


