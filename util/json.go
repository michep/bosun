package util

import (
	"bytes"

	"github.com/json-iterator/go"
	"github.com/klauspost/compress/gzip"
)

var json = jsoniter.ConfigFastest

func MarshalGzipJson(data interface{}) ([]byte, error) {
	buf := &bytes.Buffer{}
	g := gzip.NewWriter(buf)
	enc := json.NewEncoder(g)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	if err := g.Flush(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UnmarshalGzipJson(b []byte, dst interface{}) error {
	r := bytes.NewReader(b)
	g, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(g)
	return dec.Decode(dst)
}
