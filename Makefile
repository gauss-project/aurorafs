GO ?= go
GOBIND ?= gobind
GOMOBILE ?= gomobile
GOLANGCI_LINT ?= $$($(GO) env GOPATH)/bin/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.42.1
GOGOPROTOBUF ?= protoc-gen-gogofaster
GOGOPROTOBUF_VERSION ?= v1.3.2

GO_MIN_VERSION ?= 1.17
GO_BUILD_VERSION ?= 1.17.2
GO_MOD_ENABLED_VERSION ?= 1.12
GO_MOD_VERSION ?= $(shell go mod edit -print | awk '/^go[ \t]+[0-9]+\.[0-9]+(\.[0-9]+)?[ \t]*$$/{print $$2}')
GO_SYSTEM_VERSION ?= $(shell go version | awk '{ gsub(/go/, "", $$3); print $$3 }')

COMMIT_HASH ?= $(shell git describe --long --dirty --always --match "" || true)
CLEAN_COMMIT ?= $(shell git describe --long --always --match "" || true)
COMMIT_TIME ?= $(shell git show -s --format=%ct $(CLEAN_COMMIT) || true)
LDFLAGS ?= -s -w -X github.com/gauss-project/aurorafs.commitHash="$(COMMIT_HASH)" -X github.com/gauss-project/aurorafs.commitTime="$(COMMIT_TIME)"

GOOS ?= $(shell go env GOOS)
SHELL ?= bash
IS_DOCKER ?= false
DATABASE ?= wiredtiger
LIB_INSTALL_DIR ?= /usr/local

CGO_ENABLED ?= $(shell go env CGO_ENABLED)

.PHONY: all
all: build lint vet test-race binary

.PHONY: binary-wt
binary-wt: DATABASE=wiredtiger
binary-wt: binary

.PHONY: binary-ldb
binary-ldb: DATABASE=leveldb
binary-ldb: binary

.PHONY: binary
binary: dist FORCE
	$(GO) version
ifeq ($(GOOS), windows)
	$(GO) env -w CGO_ENABLED=0
	$(GO) build -tags leveldb -trimpath -ldflags "$(LDFLAGS)" -o dist/aurora.exe ./cmd/aurorafs
else
ifeq ($(DATABASE), wiredtiger)
	sh -c "./install-deps.sh $(LIB_INSTALL_DIR) $(IS_DOCKER)"
	$(GO) env -w CGO_ENABLED=1
else
	$(GO) env -w CGO_ENABLED=0
endif
	$(GO) build -tags $(DATABASE) -trimpath -ldflags "$(LDFLAGS)" -o dist/aurora ./cmd/aurorafs
endif
	$(GO) env -w CGO_ENABLED=$(CGO_ENABLED)

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
build: export CGO_ENABLED=0 || set CGO_ENABLED=0
build: check-version
build:
	$(GO) build -tags leveldb -trimpath -ldflags "$(LDFLAGS)" ./...

.PHONY: android
android: check-java
android: check-mobile-tool
android: download-vendor
android:
	[ -d "dist" ] || mkdir dist
	$(GO) mod vendor && echo "create go dependency vendor"
	[ -f "dist/aurora.aar" ] || (GO111MODULE=off $(GOMOBILE) bind -target=android -o=aurora.aar ./mobile && mv -n aurora.aar dist/ && mv -n aurora-sources.jar dist/) || (echo "build android sdk failed" && rm -rf vendor && exit 1)
	rm -rf vendor
	echo "android sdk build finished."
	echo "please import dist/aurora.aar to android studio!"

.PHONY: ios
ios: check-xcode
ios: check-mobile-tool
ios: download-vendor
ios:
	[ -d "dist" ] || mkdir dist
	$(GO) mod vendor && echo "create go dependency vendor"
	[ -d "dist/aurora.xcframework" ] || (GO111MODULE=off $(GOMOBILE) bind -target=ios -o=aurora.xcframework ./mobile && mv -n aurora.xcframework dist/) || (echo "build ios framework failed" && rm -rf vendor && exit 1)
	rm -rf vendor
	echo "ios framework build finished."
	echo "please import dist/aurora.xcframework to xcode!"

.PHONY: check-mobile-tool
check-mobile-tool: check-version
check-mobile-tool:
	check_path=false; for line in $(shell $(GO) env GOPATH | tr ':' '\n'); do if [[ $$PWD = $$line* ]]; then check_path=true; fi; done; $$check_path || (echo "Current path does not match your GOPATH, please check" && exit 1)
	type ${GOMOBILE} || $(GO) install golang.org/x/mobile/cmd/gomobile@latest
	type ${GOBIND} || $(GO) install golang.org/x/mobile/cmd/gobind@latest

.PHONY: check-java
check-java:
	type java || (echo "Not found java on the system" && exit 1)
	java -version || (echo "Java check version failed, please check java setup" && exit 1)
	[ -z $(ANDROID_HOME) ] && echo "Please set ANDROID_HOME env" && exit 1; exit 0
	[ -z $(ANDROID_NDK_HOME) ] && echo "Please install android NDK tools, and set ANDROID_NDK_HOME" && exit 1; exit 0

.PHONY: check-xcode
check-xcode:
	[ ${GOOS} = "darwin" ] || (echo "Must be on the MacOS system" && exit 1)
	xcode-select -p || (echo "Please install command line tool first" && exit 1)
	xcrun xcodebuild -version || (echo "Please install Xcode. If xcode installed, you should exec `sudo xcode-select -s /Applications/Xcode.app/Contents/Developer` in your shell" && exit 1)

.PHONY: download-vendor
download-vendor:
	$(GO) mod download
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
