FROM kno:base
MAINTAINER repoman@beingmeta.com
ENV TERM=linux
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
RUN apt-get -yqq update && apt-get -yqq full-upgrade
RUN apt-get -yqq install kno-imagetools kno-full

