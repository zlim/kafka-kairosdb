GOAPP := $(notdir $(PWD))
GOPATH := /go/$(GOAPP)

DOCKEROPTS := -e GOPATH=$(GOPATH)
DOCKEROPTS += -e GOOS=linux
DOCKEROPTS += -e CGO_ENABLED=0
DOCKEROPTS += -v $(PWD):$(GOPATH)
DOCKEROPTS += -w $(GOPATH)

dockerrun := docker run --rm $(DOCKEROPTS) golang:1.6

.DEFAULT_GOAL := all

.PHONY: deps
deps:
	$(dockerrun) go get -v -d

.PHONY: fmt
fmt:
	$(dockerrun) go fmt -x

.PHONY: clean
clean:
	$(dockerrun) go clean -x

.PHONY: build
build: deps
	$(dockerrun) go build -v -o $(GOAPP)

main: deps
	$(dockerrun) go build -v -a -o main

.PHONY: all
all: fmt clean build image

IMAGETAG := zlim/$(GOAPP)

.PHONY: image
image: ca-certificates.crt main
	docker build -t $(IMAGETAG) .

ca-certificates.crt:
	cp /etc/ssl/certs/ca-certificates.crt ca-certificates.crt

CONTAINERS_KAFKA := $(shell docker ps | grep 9092 | awk '{print $$1}')
CONTAINER_KAIROS := $(firstword $(shell docker ps | grep 8080 | awk '{print $$1}'))

KAFKA := $(addsuffix :9092,$(CONTAINERS_KAFKA))
comma := ,
KAFKA := $(subst $(eval),$(comma),$(KAFKA))
KAIROS := http://$(CONTAINER_KAIROS):8080

LINKS := $(addprefix --link ,$(CONTAINERS_KAFKA) $(CONTAINER_KAIROS))

.PHONY: run
run:
	docker run --rm $(LINKS) $(IMAGETAG) /main -kf $(KAFKA) -kdb $(KAIROS)

