FROM busybox:latest

COPY index.html /www/index.html

EXPOSE 80

CMD ["httpd", "-f", "-p", "80", "-h", "/www"]
