build:
	glide install
	go build

test:
	go test -coverprofile=main.cr
	cd vertica && go test -coverprofile=../vertica.cr
	cd ddlparser && go test -coverprofile=../ddlparser.cr
	cat main.cr > cover.profile && cat vertica.cr | tail -n +2 >> cover.profile && cat ddlparser.cr | tail -n +2 >> cover.profile
	rm main.cr vertica.cr ddlparser.cr

test-cover: test
	go tool cover -html=cover.profile

checks:
	misspell .
	ineffassign .
	golint . && golint ddlparser && golint isql && golint vertica
	gocyclo -over 12 main.go ./vertica ./ddlparser

release:
	tar -zcvf repligator-linux-amd64.tar.gz repligator

deb:
	cp repligator builds/opt/repligator/bin/repligator
	find builds -name ".gitignore" -type f -delete
	pleaserun --install-prefix builds/ --name repligator --user repligator --group repligator \
	--description "MySQL to Vertica replication service" \
	--log-directory /var/log/repligator -p sysv /opt/repligator/bin/repligator -config=/etc/repligator/config.yml
	fpm -n repligator -v `git describe --abbrev=0 --tags | cut -d "v" -f 2` -d unixodbc -d unixodbc-dev --deb-user repligator --deb-group repligator \
	--license MIT --vendor b13f@github.com -m b13f@github.com --description "MySQL to Vertica replication service" \
	--url "http://github.com/b13f/repligator" --deb-no-default-config-files \
	--pre-install builds/preinst -C builds -t deb -s dir etc opt var

clean:
	rm -f cover.profile
	rm -f repligator
	rm -rf builds/etc/default builds/etc/init.d
	rm -f builds/opt/repligator/bin/repligator

docker-build: dbuild-V7.2 dbuild-V8.0 dbuild-V8.1

dbuild-V8.1:
	docker build --rm=true -f Dockerfile \
	--build-arg REPLIGATOR_VERSION=0.1.0 \
	--build-arg VERTICA_DRIVER_ARCH_LINK=https://my.vertica.com/client_drivers/8.1.x/8.1.0-0/vertica-client-8.1.0-0.x86_64.tar.gz \
	-t b13f/repligator:r0.1.0-V8.1 .

dbuild-V8.0:
	docker build --rm=true -f Dockerfile \
	--build-arg REPLIGATOR_VERSION=0.1.0 \
	--build-arg VERTICA_DRIVER_ARCH_LINK=https://my.vertica.com/client_drivers/8.0.x/8.0.1/vertica-client-8.0.1-0.x86_64.tar.gz \
	-t b13f/repligator:r0.1.0-V8.0 .

dbuild-V7.2:
	docker build --rm=true -f Dockerfile \
	--build-arg REPLIGATOR_VERSION=0.1.0 \
	--build-arg VERTICA_DRIVER_ARCH_LINK=https://my.vertica.com/client_drivers/7.2.x/7.2.3-0/vertica-client-7.2.3-0.x86_64.tar.gz \
	-t b13f/repligator:r0.1.0-V7.2 .