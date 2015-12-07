FROM scratch

ADD dispatcher /app/

VOLUME /var/dispatcher/
EXPOSE 80

WORKDIR /app
ENTRYPOINT ["/app/dispatcher"]
