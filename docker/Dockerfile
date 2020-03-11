FROM clojure:tools-deps
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

RUN mkdir -p  /usr/share/maven/conf
#COPY maven-settings.xml /root/.m2/settings.xml


#ENV http_proxy
#ENV https_proxy

RUN apt-get update -y && apt-get install maven -y
#RUN apt-get update &&  apt-get upgrade -y &&  apt-get install -y git
RUN apt-get install -y git
RUN git clone https://github.com/Mamun/stardog-kafka.git /usr/src/app
RUN clojure -Atest

#RUN clojure -Sdeps

CMD ["/bin/sh"]

