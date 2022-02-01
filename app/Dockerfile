FROM openjdk:11
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
EXPOSE 8088
EXPOSE 8089
RUN ["chmod", "+x", "/usr/src/myapp/dockerEntry.sh"]
CMD ./dockerEntry.sh
