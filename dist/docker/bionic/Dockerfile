FROM ubuntu:18.04
MAINTAINER repoman@beingmeta.com
RUN apt-get update
RUN apt-get full-upgrade
RUN add-apt-repository 'deb [arch=any] http://repo.beingmeta.com/apt beingmeta main'
RUN add-apt-repository 'deb [arch=amd64,arch=i386] http://repo.beingmeta.com/apt bionic main'
RUN apt-add-key 'deb [arch=amd64,arch=i386] http://repo.beingmeta.com/apt bionic main'
RUN apt-get install kno-full libu8 libu8-encodings

