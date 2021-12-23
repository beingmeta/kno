;;(dynamic-load "/usr/lib/x86_64-linux-gnu/libldap.so")

(define ldopen
  (ffi/proc "ldap_open" "libldap.so" '#[typetag ldapconn ffitype ptr] 'string 'int))
(define ldbind (ffi/proc "ldap_simple_bind_s" #f 'int 
			 #[typetag ldapconn ffitype ptr] 
			 #[ffitype string nullable #t]
			 #[ffitype string nullable #t]))
(define ldsetopt
  (ffi/proc "ldap_set_option" #f 'int 
	    #[typetag ldapconn ffitype ptr] 'int 'intptr))
(define ldaperr (ffi/proc "ldap_err2string" #f 'string 'int))

(define ldapsearch 
  (ffi/proc "ldap_search_s" #f 'int
	    #[typetag ldapconn ffitype ptr] 'string 'int 'string 'stringlist 'int
	    #[typetag ldap:message ffitype resultptr]))
(define ldapcount
  (ffi/proc "ldap_count_entries" #f 'int
	    #[typetag ldapconn ffitype ptr] #[typetag ldap:message ffitype ptr]))

(define LDAP_OPTS_PROTOCOL_VERSION 0x11)
(define LDAP_SCOPE_BASE 0x00)

(define default-login "cn=admin,dc=beingmeta,dc=com")
(define default-creds "R0ci*ldap")

(define (test-result result caller)
  (unless (zero? result) (error |LDAPError| caller (ldaperr result))))

(define (ldap/open host (opts #f))
  (let ((conn (ldopen host (getopt opts 'port 389)))
	(login (getopt opts 'login default-login))
	(creds (getopt opts 'creds default-creds)))
    (test-result (ldsetopt conn LDAP_OPTS_PROTOCOL_VERSION 3)
		 "ldap/open(set_protocol)")
    (when (getopt opts 'login default-login)
      (test-result (ldbind conn
			   (getopt opts 'login default-login)
			   (getopt opts 'creds default-creds))
		   "ldap/open(login)"))
    conn))

#|
(ldopen "localhost" 389) =ld
(ldsetopt ld LDAP_OPTS_PROTOCOL_VERSION 3)
(ldbind ld "cn=admin,dc=beingmeta,dc=com" "R0ci*ldap")
|#
#|

|#
