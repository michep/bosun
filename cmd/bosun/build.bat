go generate .

set GOARCH=amd64
set GOOS=linux

rem DEBUG
rem go build .

rem PRODUCTION
go build -ldflags "-s -w" .