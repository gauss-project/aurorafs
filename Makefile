GO ?= go
GOMOBILE ?= gomobile
GOLANGCI_LINT ?= $$($(GO) env GOPATH)/bin/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.42.1
GOGOPROTOBUF ?= protoc-gen-gogofaster
GOGOPROTOBUF_VERSION ?= v1.3.2

GO_MIN_VERSION ?= "1.17"
GO_BUILD_VERSION ?= "1.17.2"
GO_MOD_ENABLED_VERSION ?= "1.12"
GO_MOD_VERSION ?= "$(shell go mod edit -print | awk '/^go[ \t]+[0-9]+\.[0-9]+(\.[0-9]+)?[ \t]*$$/{print $$2}')"
GO_SYSTEM_VERSION ?= "$(shell go version | awk '{ gsub(/go/, "", $$3); print $$3 }')"

COMMIT_HASH ?= "$(shell git describe --long --dirty --always --match "" || true)"
CLEAN_COMMIT ?= "$(shell git describe --long --always --match "" || true)"
COMMIT_TIME ?= "$(shell git show -s --format=%ct $(CLEAN_COMMIT) || true)"
LDFLAGS ?= -s -w -X github.com/gauss-project/aurorafs.commitHash="$(COMMIT_HASH)" -X github.com/gauss-project/aurorafs.commitTime="$(COMMIT_TIME)"

GOOS ?= "$(shell go env GOOS)"
ifeq ($(GOOS),"windows")
BINARY_NAME ?= aurora.exe
else
BINARY_NAME ?= aurora
endif

.PHONY: all
all: build lint vet test-race binary

.PHONY: binary
binary: export CGO_ENABLED=0
binary: dist FORCE
	$(GO) version
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o dist/$(BINARY_NAME) ./cmd/aurorafs

dist:
	mkdir $@

.PHONY: lint
lint: linter
	$(GOLANGCI_LINT) run

.PHONY: linter
linter:
	test -f $(GOLANGCI_LINT) || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $$($(GO) env GOPATH)/bin $(GOLANGCI_LINT_VERSION)

.PHONY: vet
vet:
	$(GO) vet ./...

.PHONY: test-race
test-race:
ifdef cover
	$(GO) test -race -failfast -coverprofile=cover.out -v ./...
else
	$(GO) test -race -failfast -v ./...
endif

.PHONY: test-integration
test-integration:
	$(GO) test -tags=integration -v ./...

.PHONY: test
test:
	$(GO) test -v -failfast ./...

.PHONY: build
build: export CGO_ENABLED=0
build: check-version
build:
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" ./...

.PHONY: android
android: check-java
android: check-mobile-tool
android: download-vendor
android:
	[ -d "build" ] || mkdir build
	$(GO) mod vendor && echo "create go dependency vendor"
	GO111MODULE=off $(GOMOBILE) bind -target=android -o=aurora.aar ./mobile || (echo "build failed" && rm -rf build && rm -rf vendor && exit 1)
	mv aurora.aar build/ && mv aurora-sources.jar build/
	echo "android sdk build finished."
	echo "please import build/aurora.aar to android studio!"
	rm -rf vendor

.PHONY: ios
ios: check-xcode
ios: check-mobile-tool
ios: download-vendor
ios:
	[ -d "build" ] || mkdir build
	$(GO) mod vendor && echo "create go dependency vendor"
	GO111MODULE=off $(GOMOBILE) bind -target=ios -o=aurora.framework ./mobile || (echo "build failed" && rm -rf build && rm -rf vendor && exit 1)
	mv aurora.framework build/
	echo "ios framework build finished."
	echo "please import build/aurora.framework to xcode!"
	rm -rf vendor

.PHONY: check-mobile-tool
check-mobile-tool:
	type ${GOMOBILE} || (echo "Golang mobile build tool not installed" && exit 1); exit 0

.PHONY: check-java
check-java:
	type java || (echo "Not found java on the system" && exit 1); exit 0
	java -version || (echo "Java check version failed, please check java setup" && exit 1); exit 0
	[ -z $(ANDROID_HOME) ] && echo "Please set ANDROID_HOME env" && exit 1; exit 0
	[ -z $(ANDROID_NDK_HOME) ] && echo "Please install android NDK tools, and set ANDROID_NDK_HOME" && exit 1; exit 0

.PHONY: check-xcode
check-xcode:
	[ ${GOOS} != "darwin" ] && echo "Must be on the MacOS system" && exit 1
	xcode-select -p || (echo "Please install command line tool first" && exit 1); exit 0
	xcodebuild -version || (echo "Please install Xcode" && exit 1); exit 0

.PHONY: download-vendor
download-vendor:
	$(GO) get github.com/karalabe/usb

.PHONY: check-version
check-version:
	[ ${GO_SYSTEM_VERSION} \< ${GO_MOD_ENABLED_VERSION} ] && echo "The version of Golang on the system (${GO_SYSTEM_VERSION}) is too old and does not support go modules. Please use at least ${GO_MIN_VERSION}." && exit 1; exit 0
	[ ${GO_SYSTEM_VERSION} \< ${GO_MIN_VERSION} ] && echo "The version of Golang on the system (${GO_SYSTEM_VERSION}) is below the minimum required version (${GO_MIN_VERSION}) and therefore will not build correctly." && exit 1; exit 0
	if ! expr ${GO_BUILD_VERSION} : ^${GO_MOD_VERSION} 1>/dev/null; then echo "The version of Golang mod (${GO_MOD_VERSION}) does not match required version (${GO_BUILD_VERSION})." && exit 1; fi

.PHONY: githooks
githooks:
	ln -f -s ../../.githooks/pre-push.bash .git/hooks/pre-push

.PHONY: protobuftools
protobuftools:
	which protoc || ( echo "install protoc for your system from https://github.com/protocolbuffers/protobuf/releases" && exit 1)
	which $(GOGOPROTOBUF) || ( cd /tmp && GO111MODULE=on $(GO) get -u github.com/gogo/protobuf/$(GOGOPROTOBUF)@$(GOGOPROTOBUF_VERSION) )

.PHONY: protobuf
protobuf: GOFLAGS=-mod=mod # use modules for protobuf file include option
protobuf: protobuftools
	$(GO) generate -run protoc ./...

.PHONY: clean
clean:
	$(GO) clean
	rm -rf dist/

FORCE:
