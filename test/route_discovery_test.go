// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
)

func runSeedServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/seed.conf")
}

func runAuthSeedServer(t *testing.T) (*server.Server, *server.Options) {
	return RunServerWithConfig("./configs/auth_seed.conf")
}

func TestSeedFirstRouteInfo(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc.Close()

	_, routeExpect := setupRoute(t, rc, opts)
	buf := routeExpect(infoRe)

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if len(info.Routes) != 0 {
		t.Fatalf("Expected len of []Routes to be zero vs %d\n", len(info.Routes))
	}
}

func TestSeedMultipleRouteInfo(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc1 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc1.Close()

	routeSend1, route1Expect := setupRoute(t, rc1, opts)
	route1Expect(infoRe)

	rc1ID := "2222"
	rc1Port := 22
	rc1Host := "127.0.0.1"

	hp1 := fmt.Sprintf("nats-route://%s/", net.JoinHostPort(rc1Host, strconv.Itoa(rc1Port)))

	// register ourselves via INFO
	r1Info := server.Info{ID: rc1ID, Host: rc1Host, Port: rc1Port}
	b, _ := json.Marshal(r1Info)
	infoJSON := fmt.Sprintf(server.InfoProto, b)
	routeSend1(infoJSON)
	routeSend1("PING\r\n")
	route1Expect(pongRe)

	rc2 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc2.Close()

	routeSend2, route2Expect := setupRoute(t, rc2, opts)

	rc2ID := "2224"
	rc2Port := 24
	rc2Host := "127.0.0.1"

	//	hp2 := net.JoinHostPort(rc2Host, strconv.Itoa(rc2Port))

	// register ourselves via INFO
	r2Info := server.Info{ID: rc2ID, Host: rc2Host, Port: rc2Port}
	b, _ = json.Marshal(r2Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend2(infoJSON)

	// Now read back out the info from the seed route
	buf := route2Expect(infoRe)

	infoReceived := 1
	if strings.Count(string(buf), "INFO {") == 2 {
		infoReceived = 2
		// Take the first
		infos := string(buf)
		buf = buf[0:strings.LastIndex(infos, "INFO {")]
	}

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if len(info.Routes) != 1 {
		t.Fatalf("Expected len of []Routes to be 1 vs %d\n", len(info.Routes))
	}

	route := info.Routes[0]
	if route.RemoteID != rc1ID {
		t.Fatalf("Expected RemoteID of \"22\", got %q\n", route.RemoteID)
	}
	if route.URL == "" {
		t.Fatalf("Expected a URL for the implicit route")
	}
	if route.URL != hp1 {
		t.Fatalf("Expected URL Host of %s, got %s\n", hp1, route.URL)
	}

	if infoReceived == 1 {
		route2Expect(infoRe)
	}

	routeSend2("PING\r\n")
	route2Expect(pongRe)

	// Now let's do a third.
	rc3 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc3.Close()

	routeSend3, route3Expect := setupRoute(t, rc3, opts)

	rc3ID := "2226"
	rc3Port := 26
	rc3Host := "127.0.0.1"

	// register ourselves via INFO
	r3Info := server.Info{ID: rc3ID, Host: rc3Host, Port: rc3Port}
	b, _ = json.Marshal(r3Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend3(infoJSON)

	// Now read back out the info from the seed route
	buf = route3Expect(infoRe)
	if strings.Count(string(buf), "INFO {") == 2 {
		// Take the first
		infos := string(buf)
		buf = buf[0:strings.LastIndex(infos, "INFO {")]
	}

	info = server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}
	if len(info.Routes) != 2 {
		t.Fatalf("Expected len of []Routes to be 2 vs %d\n", len(info.Routes))
	}
}

func TestSeedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.ClusterHost, opts.ClusterPort)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHttpRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.Id(), s3.Id()})
	if ris[s2.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s3.Id()})
	if ris[s1.Id()].IsConfigured != true {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s2.Id()})
	if ris[s1.Id()].IsConfigured != true {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s2.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}
}

type serverInfo struct {
	server *server.Server
	opts   *server.Options
}

func checkConnected(t *testing.T, servers []serverInfo, current int) error {
	s := servers[current]

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", s.opts.Host, s.opts.HTTPPort)
	rz := readHttpRoutez(t, url)
	total := len(servers)
	var ids []string
	for i := 0; i < total; i++ {
		if i == current {
			continue
		}
		ids = append(ids, servers[i].server.Id())
	}
	ris, err := expectRidsNoFatal(t, true, rz, ids)
	if err != nil {
		return err
	}
	for i := 0; i < total; i++ {
		if i == current {
			continue
		}
		s := servers[i]
		if i == 0 && ris[s.server.Id()].IsConfigured == false {
			return errors.New(fmt.Sprintf("Expected server %s:%d to be configured", s.opts.Host, s.opts.Port))
		} else if i > 0 {
			if ris[s.server.Id()].IsConfigured == true {
				return errors.New(fmt.Sprintf("Expected server %s:%d not to be configured", s.opts.Host, s.opts.Port))
			}
		}
	}
	return nil
}

func TestStressSeedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.ClusterHost, opts.ClusterPort)

	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s3Opts := nextServerOpts(s2Opts)
	s4Opts := nextServerOpts(s3Opts)

	for i := 0; i < 10; i++ {
		func() {
			// Run these servers manually, because we want them to start and
			// connect to s1 as fast as possible.

			s2 := server.New(s2Opts)
			if s2 == nil {
				panic("No NATS Server object returned.")
			}
			defer s2.Shutdown()
			go s2.Start()

			s3 := server.New(s3Opts)
			if s3 == nil {
				panic("No NATS Server object returned.")
			}
			defer s3.Shutdown()
			go s3.Start()

			s4 := server.New(s4Opts)
			if s4 == nil {
				panic("No NATS Server object returned.")
			}
			defer s4.Shutdown()
			go s4.Start()

			serversInfo := []serverInfo{{s1, opts}, {s2, s2Opts}, {s3, s3Opts}, {s4, s4Opts}}

			var err error
			maxTime := time.Now().Add(5 * time.Second)
			for time.Now().Before(maxTime) {
				resetPreviousHTTPConnections()

				for j := 0; j < len(serversInfo); j++ {
					err = checkConnected(t, serversInfo, j)
					// If error, start this for loop from beginning
					if err != nil {
						break
					}
				}
				// All servers checked ok, we are done, otherwise, try again
				// until time is up
				if err == nil {
					break
				}
			}
			// Report error
			if err != nil {
				t.Fatalf("Error: %v", err)
			}
		}()
	}
}

func TestChainedSolicitWorks(t *testing.T) {
	s1, opts := runSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%d/", opts.ClusterHost, opts.ClusterPort)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)
	// We will have s3 connect to s2, not the seed.
	routesStr = fmt.Sprintf("nats-route://%s:%d/", s2Opts.ClusterHost, s2Opts.ClusterPort)
	s3Opts.Routes = server.RoutesFromStr(routesStr)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHttpRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.Id(), s3.Id()})
	if ris[s2.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s3.Id()})
	if ris[s1.Id()].IsConfigured != true {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s2.Id()})
	if ris[s2.Id()].IsConfigured != true {
		t.Fatalf("Expected s2 server to be configured\n")
	}
	if ris[s1.Id()].IsConfigured == true {
		t.Fatalf("Expected seed server not to be configured\n")
	}
}

func TestAuthSeedSolicitWorks(t *testing.T) {
	s1, opts := runAuthSeedServer(t)
	defer s1.Shutdown()

	// Create the routes string for others to connect to the seed.
	routesStr := fmt.Sprintf("nats-route://%s:%s@%s:%d/", opts.ClusterUsername, opts.ClusterPassword, opts.ClusterHost, opts.ClusterPort)

	// Run Server #2
	s2Opts := nextServerOpts(opts)
	s2Opts.Routes = server.RoutesFromStr(routesStr)

	s2 := RunServer(s2Opts)
	defer s2.Shutdown()

	// Run Server #3
	s3Opts := nextServerOpts(s2Opts)

	s3 := RunServer(s3Opts)
	defer s3.Shutdown()

	// Wait for a bit for graph to connect
	time.Sleep(500 * time.Millisecond)

	// Grab Routez from monitor ports, make sure we are fully connected
	url := fmt.Sprintf("http://%s:%d/", opts.Host, opts.HTTPPort)
	rz := readHttpRoutez(t, url)
	ris := expectRids(t, rz, []string{s2.Id(), s3.Id()})
	if ris[s2.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s2Opts.Host, s2Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s3.Id()})
	if ris[s1.Id()].IsConfigured != true {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s3.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}

	url = fmt.Sprintf("http://%s:%d/", s3Opts.Host, s3Opts.HTTPPort)
	rz = readHttpRoutez(t, url)
	ris = expectRids(t, rz, []string{s1.Id(), s2.Id()})
	if ris[s1.Id()].IsConfigured != true {
		t.Fatalf("Expected seed server to be configured\n")
	}
	if ris[s2.Id()].IsConfigured == true {
		t.Fatalf("Expected server not to be configured\n")
	}
}

// Helper to check for correct route memberships
func expectRids(t *testing.T, rz *server.Routez, rids []string) map[string]*server.RouteInfo {
	ri, err := expectRidsNoFatal(t, false, rz, rids)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return ri
}

func expectRidsNoFatal(t *testing.T, direct bool, rz *server.Routez, rids []string) (map[string]*server.RouteInfo, error) {
	caller := 1
	if !direct {
		caller++
	}
	if len(rids) != rz.NumRoutes {
		_, fn, line, _ := runtime.Caller(caller)
		return nil, errors.New(fmt.Sprintf("[%s:%d] Expecting %d routes, got %d\n", fn, line, len(rids), rz.NumRoutes))
	}
	set := make(map[string]bool)
	for _, v := range rids {
		set[v] = true
	}
	// Make result map for additional checking
	ri := make(map[string]*server.RouteInfo)
	for _, r := range rz.Routes {
		if set[r.RemoteId] != true {
			_, fn, line, _ := runtime.Caller(caller)
			return nil, errors.New(fmt.Sprintf("[%s:%d] Route with rid %s unexpected, expected %+v\n", fn, line, r.RemoteId, rids))
		}
		ri[r.RemoteId] = r
	}
	return ri, nil
}

// Helper to easily grab routez info.
func readHttpRoutez(t *testing.T, url string) *server.Routez {
	resp, err := http.Get(url + "routez")
	if err != nil {
		t.Fatalf("Expected no error: Got %v\n", err)
	}
	if resp.StatusCode != 200 {
		// Do one retry - FIXME(dlc) - Why does this fail when running the solicit tests b2b?
		resp, _ = http.Get(url + "routez")
		if resp.StatusCode != 200 {
			t.Fatalf("Expected a 200 response, got %d\n", resp.StatusCode)
		}
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Got an error reading the body: %v\n", err)
	}
	r := server.Routez{}
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("Got an error unmarshalling the body: %v\n", err)
	}
	return &r
}

func TestSeedReturnIPInsteadOfURL(t *testing.T) {
	s, opts := runSeedServer(t)
	defer s.Shutdown()

	rc1 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc1.Close()

	routeSend1, route1Expect := setupRoute(t, rc1, opts)
	route1Expect(infoRe)

	rc1ID := "2222"
	rc1Port := 22
	rc1Host := "localhost"

	// register ourselves via INFO
	r1Info := server.Info{ID: rc1ID, Host: rc1Host, Port: rc1Port}
	b, _ := json.Marshal(r1Info)
	infoJSON := fmt.Sprintf(server.InfoProto, b)
	routeSend1(infoJSON)
	routeSend1("PING\r\n")
	route1Expect(pongRe)

	rc2 := createRouteConn(t, opts.ClusterHost, opts.ClusterPort)
	defer rc2.Close()

	routeSend2, route2Expect := setupRoute(t, rc2, opts)

	rc2ID := "2224"
	rc2Port := 24
	rc2Host := "localhost"

	// register ourselves via INFO
	r2Info := server.Info{ID: rc2ID, Host: rc2Host, Port: rc2Port}
	b, _ = json.Marshal(r2Info)
	infoJSON = fmt.Sprintf(server.InfoProto, b)
	routeSend2(infoJSON)

	// Now read back out the info from the seed route
	buf := route2Expect(infoRe)
	if strings.Count(string(buf), "INFO {") == 2 {
		// Take the first
		infos := string(buf)
		buf = buf[0:strings.LastIndex(infos, "INFO {")]
	}

	info := server.Info{}
	if err := json.Unmarshal(buf[4:], &info); err != nil {
		t.Fatalf("Could not unmarshal route info: %v", err)
	}

	if len(info.Routes) != 1 {
		t.Fatalf("Expected len of []Routes to be 1 vs %d", len(info.Routes))
	}

	route := info.Routes[0]
	if route.RemoteID != rc1ID {
		t.Fatalf("Expected RemoteID of \"22\", got %q", route.RemoteID)
	}
	if route.URL == "" {
		t.Fatal("Expected a URL for the implicit route")
	}
	rurl := strings.TrimPrefix(route.URL, "nats-route://")
	rhost, _, err := net.SplitHostPort(rurl)
	if err != nil {
		t.Fatalf("Error getting host information from: %v, err=%v", route.URL, err)
	}
	if rhost != rc1Host {
		t.Fatalf("Expected route's host to be %v, got %v", rc1Host, rhost)
	}

	if route.IP == "" {
		t.Fatal("Expected an IP for the implicit route")
	}
	ripurl := strings.TrimPrefix(route.IP, "nats-route://")
	rip, _, err := net.SplitHostPort(ripurl)
	if err != nil {
		t.Fatalf("Error getting hipost information from: %v, err=%v", route.IP, err)
	}
	if rip == rc1Host {
		t.Fatalf("Expected route's ip to be different from host, got %v", rip)
	}
	addr, ok := rc1.RemoteAddr().(*net.TCPAddr)
	if !ok {
		t.Fatal("Unable to get IP address from route")
	}
	if rip != addr.IP.String() {
		t.Fatalf("Expected IP %s, got %s", addr.IP.String(), rhost)
	}
}
