FROM scratch

ADD dispatcher /app/
ADD config.yml /app/

VOLUME /var/dispatcher/
EXPOSE 80

WORKDIR /app
ENTRYPOINT ["/app/dispatcher"]
