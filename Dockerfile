FROM adoptopenjdk/openjdk11:alpine-jre

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-dao-cassandra" \
	PARA_PLUGIN_VER="1.38.3"

ADD https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
