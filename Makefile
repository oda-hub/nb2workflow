nb2wrev:=$(shell git describe --always --tags)
image:=odahub/nb2workflow:$(nb2wrev)

build: Dockerfile
	docker build --build-arg nb2workflow_revision=$(nb2wrev) -t $(image) . 

push: build
	docker push $(image) 
