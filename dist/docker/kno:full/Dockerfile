FROM ubuntu:18.04
MAINTAINER repoman@beingmeta.com
RUN apt-get -yqq update
RUN apt-get -yqq install apt-utils zile software-properties-common
RUN apt-get -yqq full-upgrade
RUN apt-key adv --receive-keys FE1BC737F9F323D732AA26330620266BE5AFF294
RUN add-apt-repository 'deb [arch=any] https://repo.beingmeta.com/apt beingmeta main'
RUN add-apt-repository 'deb [arch=amd64,arch=i386.arch=arm64,arch=armhf] https://repo.beingmeta.com/apt bionic main'
RUN apt-get -yqq install libu8 libu8-encodings
RUN apt-get -yqq install kno-core kno-stdlib kno-parseltongue
RUN apt-get -yqq install kno-sqlite kno-zeromq kno-leveldb kno-rocksdb kno-sqlite kno-mysql kno-mongodb
RUN apt-get -yqq install kno-sundown kno-tidy 
