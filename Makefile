GIT = $(shell git rev-parse --git-dir | xargs readlink -f)
ROOT = $(shell readlink -f ${GIT}/../)

all:
	go get github.com/armen/goviral/zgossip

test:
	go test github.com/armen/goviral/zgossip

zgissip-msg:
	gsl -script:zproto_codec_go zgossip_msg.xml

gofmt-hook:
	cp ${ROOT}/misc/gofmt-hook/pre-commit ${GIT}/hooks/
	chmod +x ${GIT}/hooks/pre-commit
