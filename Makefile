# This file is part of the RonDB REST API Server
# Copyright (c) 2022 Hopsworks AB
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 3.
# 
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`
BRANCH=`git rev-parse --abbrev-ref HEAD`
VERSION=`grep "\bVERSION\b" version/version.go  | grep -o "[0-9.]*"`
SERVER_BIN="rondb-rest-api-server"

DIRS = $(shell find . -type d)
FILES = $(shell find . -type f -iname '*.go')

all: rondb-rest-api-server

#-tags osusergo,netgo
rondb-rest-api-server: $(DIRS) $(FILES)
	go build  -ldflags="-w \
		-X hopsworks.ai/rondb-rest-api-server/version.GITCOMMIT=${GITCOMMIT} \
		-X hopsworks.ai/rondb-rest-api-server/version.BUILDTIME=${BUILDTIME} \
		-X hopsworks.ai/rondb-rest-api-server/version.HOSTNAME=${HOSTNAME} \
		-X hopsworks.ai/rondb-rest-api-server/version.BRANCH=${BRANCH}" \
		-o ./bin/server/$(VERSION)/$(SERVER_BIN) ./cmd/server/main.go 

clean:
	rm -rf ./bin/*

test: 
	go test ./... -coverprofile coverage.out 
	go tool cover -html=coverage.out -o coverage.html && xdg-open coverage.html
