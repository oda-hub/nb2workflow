image:=odahub/nb2workflow:$(shell git describe --always --tags)

build: Dockerfile
	docker build -t $(image) . 

push: build
	docker push $(image) 
