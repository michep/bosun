rem go generate .

set GOARCH=amd64
set GOOS=linux

rem DEBUG
go build .

rem PRODUCTION
rem go build -ldflags "-s -w" .