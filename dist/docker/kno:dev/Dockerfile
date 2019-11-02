FROM ubuntu:18.04
MAINTAINER repoman@beingmeta.com
RUN apt-get -yqq update
RUN apt-get -yqq install apt-utils zile software-properties-common tzdata
RUN apt-get -yqq full-upgrade
RUN apt-key adv --receive-keys FE1BC737F9F323D732AA26330620266BE5AFF294
RUN add-apt-repository 'deb [arch=any] https://repo.beingmeta.com/apt beingmeta main'
RUN add-apt-repository 'deb [arch=amd64,arch=i386.arch=arm64,arch=armhf] https://repo.beingmeta.com/apt bionic main'
RUN apt-get -yqq install build-essential make cmake autoconf autoconf-archive \
    	    	 gcc gdb gcovr lcov gettext gettext-base gettext-el doxygen doxygen-latex
RUN apt-get -yqq install libu8 libu8-encodings libu8-dev
RUN apt-get -yqq install \
    	    debhelper autotools-dev equivs dh-systemd dh-elpa debsigs devscripts \
    	    libgoogle-perftools-dev libjemalloc-dev libsnappy-dev libzstd-dev libarchive-dev libzip-dev \
	    libexif-dev libqrencode-dev libmagickwand-dev ronn \
	    python-dev libpython2.7-dev python3-dev libpython3-dev \
	    libhyphen-dev libedit-dev libsasl2-dev mongo-c-driver \
	    sqlite3  libmysqlclient-dev libmongoc-dev libbson-dev libleveldb-dev librocksdb-dev libsqlite3-dev \
	    apache2-dev libcurl4-openssl-dev libldns-dev
RUN apt-get -yqq install kno-full kno-dev
