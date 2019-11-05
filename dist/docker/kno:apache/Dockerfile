FROM ubuntu:18.04
MAINTAINER repoman@beingmeta.com
RUN for dir in /data /src /app /kno /data/brico /data/wikid /data/lexdata; do if test ! -d ${dir}; then mkdir ${dir}; fi; done;
ENV TERM=linux
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
MAINTAINER repoman@beingmeta.com
RUN apt-get -yqq update && \
    apt-get -yqq install apt-utils zile software-properties-common tzdata && \
    apt-get -yqq full-upgrade
RUN apt-get -yqq install libu8 libu8-encodings \
    	    	 	 kno-core kno-stdlib kno-parseltongue \
     	     	         kno-sqlite kno-zeromq kno-leveldb kno-rocksdb kno-sqlite kno-mysql kno-mongodb \
			 kno-sundown kno-tidy
RUN apt-get -yqq install apache2 libapache2-mod-knocgi
RUN a2enmod ssl rewrite status proxy proxy_http info headers knocgi alias auth_basic auth_digest mime
