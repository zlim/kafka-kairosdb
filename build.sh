GOAPP=`basename $PWD`
GOPATH=/go/$GOAPP

docker run --rm -e GOPATH=$GOPATH -v "$PWD":$GOPATH -w $GOPATH golang:1.6 go get -v
docker run --rm -e GOPATH=$GOPATH -v "$PWD":$GOPATH -w $GOPATH golang:1.6 go fmt -x
docker run --rm -e GOPATH=$GOPATH -v "$PWD":$GOPATH -w $GOPATH golang:1.6 go build -v
