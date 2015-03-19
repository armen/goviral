GIT = $(shell git rev-parse --git-dir | xargs readlink -f)
ROOT = $(shell readlink -f ${GIT}/../)

all:
	go get github.com/armen/goviral/zgossip

test:
	go test github.com/armen/goviral/zgossip

generate:
	gsl zgossip_msg.xml
	gsl zgossip.xml

gofmt-hook:
	cp ${ROOT}/misc/gofmt-hook/pre-commit ${GIT}/hooks/
	chmod +x ${GIT}/hooks/pre-commit
