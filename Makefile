image:=odahub/nb2workflow

build: Dockerfile
	docker build -t $(image) . 

push: build
	docker push $(image) 
