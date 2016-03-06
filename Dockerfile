FROM scratch
MAINTAINER Zi Shen Lim <zlim.lnx@gmail.com>
ADD ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ADD main /main
CMD ["/main"]
