package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"strings"

	"github.com/buaazp/fasthttprouter"
	"github.com/valyala/fasthttp"

	"bosun.org/_version"
	"bosun.org/collect"
	"bosun.org/metadata"
	"bosun.org/opentsdb"
	"bosun.org/slog"
)

var (
	listenAddr      = flag.String("l", ":4242", "Listen address.")
	bosunServer     = flag.String("b", "bosun", "Target Bosun server. Can specify port with host:port.")
	secondaryRelays = flag.String("r", "", "Additional relays to send data to. Intended for secondary data center replication. Only response from primary tsdb server wil be relayed to clients.")
	tsdbServer      = flag.String("t", "", "Target OpenTSDB server. Can specify port with host:port.")
	logVerbose      = flag.Bool("v", false, "enable verbose logging")
	flagVersion     = flag.Bool("version", false, "Prints the version and exits.")
	pprofEnalbled   = flag.Bool("pprof", false, "Enable pprof web interface.")
	pprofListenAddr = flag.String("p", ":6060", "Pprof listen address.")
)

var (
	bosunIndexURL     string
	relayDataUrls     []*url.URL
	relayMetadataUrls []*url.URL
	tags              = opentsdb.TagSet{}
	fastclient        *fasthttp.Client
)

func init() {
	fastclient = &fasthttp.Client{
		Name: "Tsdbrelay/" + version.ShortVersion(),
	}
}

func main() {
	var err error
	myHost, err = os.Hostname()
	if err != nil || myHost == "" {
		myHost = "tsdbrelay"
	}

	flag.Parse()
	if *flagVersion {
		fmt.Println(version.GetVersionInfo("tsdbrelay"))
		os.Exit(0)
	}
	if *bosunServer == "" || *tsdbServer == "" {
		slog.Fatal("must specify both bosun and tsdb server")
	}
	slog.Infoln(version.GetVersionInfo("tsdbrelay"))
	slog.Infoln("listen on", *listenAddr)
	slog.Infoln("relay to bosun at", *bosunServer)
	slog.Infoln("relay to tsdb at", *tsdbServer)

	tsdbURL, err := parseHost(*tsdbServer, "", true)
	if err != nil {
		slog.Fatalf("Invalid -t value: %s", err)
	}
	u := *tsdbURL
	u.Path = "/api/put"

	bosunURL, err := parseHost(*bosunServer, "", true)
	if err != nil {
		slog.Fatalf("Invalid -b value: %s", err)
	}
	u = *bosunURL
	u.Path = "/api/index"
	bosunIndexURL = u.String()
	if *secondaryRelays != "" {
		for _, rURL := range strings.Split(*secondaryRelays, ",") {
			u, err := parseHost(rURL, "/api/put", false)
			if err != nil {
				slog.Fatalf("Invalid -r value '%s': %s", rURL, err)
			}
			f := u.Fragment
			u.Fragment = ""
			if f == "" || strings.ToLower(f) == "data-only" {
				relayDataUrls = append(relayDataUrls, u)
			}
			if f == "" || strings.ToLower(f) == "metadata-only" || strings.ToLower(f) == "bosun-index" {
				u.Path = "/api/metadata/put"
				relayMetadataUrls = append(relayMetadataUrls, u)
			}
			if strings.ToLower(f) == "bosun-index" {
				u.Path = "/api/index"
				relayDataUrls = append(relayDataUrls, u)
			}
		}
	}

	router := fasthttprouter.New()

	tsdbProxy := &fasthttp.HostClient{
		Addr: tsdbURL.Host,
		Name: "Tsdbrelay/" + version.ShortVersion(),
	}
	bosunProxy := &fasthttp.HostClient{
		Addr: bosunURL.Host,
		Name: "Tsdbrelay/" + version.ShortVersion(),
	}
	rp := &relayProxy{
		TSDBProxy:  tsdbProxy,
		BosunProxy: bosunProxy,
	}

	put := func(ctx *fasthttp.RequestCtx) {
		rp.fastRelayPut(ctx)
	}
	router.POST("/api/put", put)

	meta := func(ctx *fasthttp.RequestCtx) {
		rp.fastRelayMetadata(ctx)
	}
	router.POST("/api/metadata/put", meta)

	router.GET("/", func(ctx *fasthttp.RequestCtx) {})

	collectUrl := &url.URL{
		Scheme: "http",
		Host:   *listenAddr,
		Path:   "/api/put",
	}
	if err = collect.Init(collectUrl, "tsdbrelay"); err != nil {
		slog.Fatal(err)
	}
	if err := metadata.Init(collectUrl, false); err != nil {
		slog.Fatal(err)
	}
	// Make sure these get zeroed out instead of going unknown on restart
	collect.Add("puts.relayed", tags, 0)
	collect.Add("puts.error", tags, 0)
	collect.Add("metadata.relayed", tags, 0)
	collect.Add("metadata.error", tags, 0)
	collect.Add("additional.puts.relayed", tags, 0)
	collect.Add("additional.puts.error", tags, 0)
	metadata.AddMetricMeta("tsdbrelay.puts.relayed", metadata.Counter, metadata.Count, "Number of successful puts relayed to opentsdb target")
	metadata.AddMetricMeta("tsdbrelay.puts.error", metadata.Counter, metadata.Count, "Number of puts that could not be relayed to opentsdb target")
	metadata.AddMetricMeta("tsdbrelay.metadata.relayed", metadata.Counter, metadata.Count, "Number of successful metadata puts relayed to bosun target")
	metadata.AddMetricMeta("tsdbrelay.metadata.error", metadata.Counter, metadata.Count, "Number of metadata puts that could not be relayed to bosun target")
	metadata.AddMetricMeta("tsdbrelay.additional.puts.relayed", metadata.Counter, metadata.Count, "Number of successful puts relayed to additional targets")
	metadata.AddMetricMeta("tsdbrelay.additional.puts.error", metadata.Counter, metadata.Count, "Number of puts that could not be relayed to additional targets")

	if *pprofEnalbled {
		go func() {
			slog.Infoln("pprof enabled and listen on", *pprofListenAddr)
			http.ListenAndServe(*pprofListenAddr, nil)
		}()
	}

	slog.Fatal(fasthttp.ListenAndServe(*listenAddr, router.Handler))
}

func verbose(format string, a ...interface{}) {
	if *logVerbose {
		slog.Infof(format, a...)
	}
}

type relayProxy struct {
	TSDBProxy  *fasthttp.HostClient
	BosunProxy *fasthttp.HostClient
}

var (
	relayHeader = "X-Relayed-From"
	myHost      string
)

func (rp *relayProxy) fastRelayPut(ctx *fasthttp.RequestCtx) {
	isRelayed := ctx.Request.Header.Peek(relayHeader) != nil
	req := &ctx.Request
	resp := &ctx.Response
	req.Header.Del("Connection")
	err := rp.TSDBProxy.Do(req, resp)
	if err != nil || resp.Header.StatusCode()/100 != 2 {
		verbose("ERROR relayPut got status: %d, error: %v, target: %v", resp.Header.StatusCode(), err, rp.TSDBProxy.Addr+string(req.URI().Path()))
		collect.Add("puts.error", tags, 1)
		return
	}
	verbose("relayed to tsdb, status: %v, target: %v", resp.Header.StatusCode(), rp.TSDBProxy.Addr+string(req.URI().Path()))
	collect.Add("puts.relayed", tags, 1)

	bosunIndexReq := fasthttp.AcquireRequest()
	req.BodyWriteTo(bosunIndexReq.BodyWriter())
	req.Header.CopyTo(&bosunIndexReq.Header)

	// Send to bosun in a separate go routine so we can end the source's request.
	go func(req *fasthttp.Request) {
		resp := fasthttp.AcquireResponse()
		uri := fasthttp.AcquireURI()
		defer func() {
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			fasthttp.ReleaseURI(uri)
		}()
		uri.Parse(nil, []byte(bosunIndexURL))
		req.SetRequestURIBytes(uri.Path())
		err := rp.BosunProxy.Do(req, resp)
		if err != nil {
			verbose("ERROR bosun relay error: %v, target: %v", err, rp.BosunProxy.Addr+string(req.URI().Path()))
			return
		}
		verbose("bosun relay success, status: %v, target: %v", resp.Header.StatusCode(), rp.BosunProxy.Addr+string(req.URI().Path()))
	}(bosunIndexReq)

	if !isRelayed && len(relayDataUrls) > 0 {
		relayReq := fasthttp.AcquireRequest()
		req.BodyWriteTo(relayReq.BodyWriter())
		req.Header.CopyTo(&relayReq.Header)
		relayReq.Header.Add(relayHeader, myHost)
		go func(req *fasthttp.Request) {
			resp := fasthttp.AcquireResponse()
			uri := fasthttp.AcquireURI()
			defer func() {
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
				fasthttp.ReleaseURI(uri)
			}()
			for _, relayURL := range relayDataUrls {
				uri.Parse([]byte(relayURL.Host), []byte(relayURL.Path))
				req.SetRequestURIBytes(uri.FullURI())
				err := fastclient.Do(req, resp)
				if err != nil {
					verbose("ERROR secondary relay error: %v, target: %v", err, string(req.URI().FullURI()))
					collect.Add("additional.puts.error", tags, 1)
					continue
				}
				verbose("secondary relay success, status: %v, target: %v", resp.Header.StatusCode(), string(req.URI().FullURI()))
				collect.Add("additional.puts.relayed", tags, 1)
			}
		}(relayReq)
	}
}

func (rp *relayProxy) fastRelayMetadata(ctx *fasthttp.RequestCtx) {
	req := &ctx.Request
	resp := &ctx.Response
	err := rp.BosunProxy.Do(req, resp)
	if err != nil || resp.Header.StatusCode() != 204 {
		verbose("ERROR relayMetadata got status %d, target: %v", resp.Header.StatusCode(), rp.TSDBProxy.Addr+string(req.URI().Path()))
		collect.Add("metadata.error", tags, 1)
		return
	}
	verbose("relayed metadata to bosun, status: %v, target: %v", resp.Header.StatusCode(), rp.TSDBProxy.Addr+string(req.URI().Path()))
	collect.Add("metadata.relayed", tags, 1)
	if req.Header.Peek(relayHeader) != nil {
		return
	}
	if len(relayMetadataUrls) != 0 {
		relayReq := fasthttp.AcquireRequest()
		req.BodyWriteTo(relayReq.BodyWriter())
		req.Header.CopyTo(&relayReq.Header)
		relayReq.Header.Add(relayHeader, myHost)
		go func(req *fasthttp.Request) {
			resp := fasthttp.AcquireResponse()
			uri := fasthttp.AcquireURI()
			defer func() {
				fasthttp.ReleaseRequest(req)
				fasthttp.ReleaseResponse(resp)
				fasthttp.ReleaseURI(uri)
			}()
			for _, relayURL := range relayMetadataUrls {
				uri.Parse([]byte(relayURL.Host), []byte(relayURL.Path))
				req.SetRequestURIBytes(uri.FullURI())
				err := fastclient.Do(req, resp)
				if err != nil {
					verbose("ERROR secondary relay metadata error: %v, target: %v", err, string(req.URI().FullURI()))
					continue
				}
				verbose("secondary relay metadata success, status: %v, target: %v", resp.Header.StatusCode(), string(req.URI().FullURI()))
			}
		}(relayReq)
	}
}

// Parses a url of the form proto://host:port/path#fragment with the following rules:
// proto:// is optional and will default to http:// if omitted
// :port is optional and will use the default if omitted
// /path is optional and will be ignored, will always be replaced by newpath
// #fragment is optional and will be removed if removeFragment is true
func parseHost(host string, newpath string, removeFragment bool) (*url.URL, error) {
	if !strings.Contains(host, "//") {
		host = "http://" + host
	}
	u, err := url.Parse(host)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, fmt.Errorf("no host specified")
	}
	u.Path = newpath
	if removeFragment {
		u.Fragment = ""
	}
	return u, nil
}
