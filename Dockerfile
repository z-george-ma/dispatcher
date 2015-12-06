FROM scratch

ADD /bin/dispatcher /app/
ADD config.yml /app/

WORKDIR /app
EXPOSE 80
ENTRYPOINT ["/app/dispatcher"]
