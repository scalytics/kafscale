FROM docker:27-cli

RUN apk add --no-cache curl bash git
RUN curl -sL https://raw.githubusercontent.com/nektos/act/master/install.sh | bash \
    && mv ./bin/act /usr/local/bin/act

WORKDIR /workspace
ENTRYPOINT ["act"]
