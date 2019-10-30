FROM ubuntu:18.04
MAINTAINER repoman@beingmeta.com
RUN apt-get -y update
RUN apt-get -y install apt-utils zile software-properties-common
RUN apt-get -y dist-upgrade
RUN apt-key adv --receive-keys FE1BC737F9F323D732AA26330620266BE5AFF294
RUN add-apt-repository 'deb [arch=any] http://repo.beingmeta.com/apt beingmeta main'
RUN add-apt-repository 'deb [arch=amd64,arch=i386] http://repo.beingmeta.com/apt bionic main'
RUN apt-get -y install kno-full libu8 libu8-encodings

