FROM openjdk:alpine as JolieBuild

# Download and install Jolie. We need it for running the release tools.
RUN apk update && apk add --no-cache wget
RUN wget https://www.jolie-lang.org/files/releases/jolie-1.8.2.jar
RUN java -jar jolie-1.8.2.jar -jh /jolie_home/ -jl /jolie_executables/

# ENV JOLIE_HOME /usr/lib/jolie


# Go build environment
FROM golang:1.13.5-alpine as goBuild

RUN apk add git
COPY go /build/
WORKDIR /build/messaging
#RUN go test
WORKDIR /build
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s" -a -installsuffix cgo -o main


# Final image
FROM ubuntu:18.04
# Needed packages 
RUN apt update && apt upgrade -y && apt install -y iptables openjdk-11-jre 

# Limited rights user
RUN useradd -m -s /bin/bash -U no-internet

# Final jolie stuff
COPY --from=JolieBuild /jolie_executables /usr/lib/jolie_executables
COPY --from=JolieBuild /jolie_home /usr/lib/jolie_home
RUN ln -s /usr/lib/jolie_executables/jolie /usr/bin/jolie
ENV JOLIE_HOME /usr/lib/jolie_home

COPY build/jolie_home.txt .
RUN cat jolie_home.txt >> /etc/environment

# Final go stuff
COPY --from=goBuild /build/main jolie-exec
RUN chmod +x jolie-exec

COPY build/ni.sh /usr/bin/ni
COPY build/iptables_no-internet_rule.sh /etc/network/if-pre-up.d/iptables_no-internet_rule

COPY build/limits.conf .
RUN cat limits.conf >> /etc/security/limits.conf
RUN rm limits.conf

RUN chmod +x /usr/bin/ni
RUN chmod +x /etc/network/if-pre-up.d/iptables_no-internet_rule


# Ensure no write permissions for user executing code
RUN chown -R root:root /*  2>/dev/null || echo "[ COMPLETE ] chown -R root /*"
RUN chmod 700 /var


CMD [ "sh", "-c", "./etc/network/if-pre-up.d/iptables_no-internet_rule && ./jolie-exec" ]
