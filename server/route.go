// Copyright 2013-2015 Apcera Inc. All rights reserved.

package server

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Designate the router type
type RouteType int

// Type of Route
const (
	// This route we learned from speaking to other routes.
	Implicit RouteType = iota
	// This route was explicitly configured.
	Explicit
)

type route struct {
	remoteID     string
	didSolicit   bool
	routeType    RouteType
	url          *url.URL
	ipUrlString  string
	authRequired bool
	tlsRequired  bool
	updatedInfo  bool
}

type RemoteInfo struct {
	RemoteID     string `json:"id"`
	URL          string `json:"url"`
	IP           string `json:"ip_url"`
	AuthRequired bool   `json:"auth_required"`
	TLSRequired  bool   `json:"tls_required"`
}

type connectInfo struct {
	Verbose  bool   `json:"verbose"`
	Pedantic bool   `json:"pedantic"`
	User     string `json:"user,omitempty"`
	Pass     string `json:"pass,omitempty"`
	TLS      bool   `json:"tls_required"`
	Name     string `json:"name"`
}

const (
	ConProto  = "CONNECT %s" + _CRLF_
	InfoProto = "INFO %s" + _CRLF_
)

// Lock should be held entering here.
func (c *client) sendConnect(tlsRequired bool) {
	var user, pass string
	if userInfo := c.route.url.User; userInfo != nil {
		user = userInfo.Username()
		pass, _ = userInfo.Password()
	}
	cinfo := connectInfo{
		Verbose:  false,
		Pedantic: false,
		User:     user,
		Pass:     pass,
		TLS:      tlsRequired,
		Name:     c.srv.info.ID,
	}
	b, err := json.Marshal(cinfo)
	if err != nil {
		c.Errorf("Error marshalling CONNECT to route: %v\n", err)
		c.closeConnection()
		return
	}
	c.bw.WriteString(fmt.Sprintf(ConProto, b))
	c.bw.Flush()
}

// Assume the server lock is held upon entry.
func (s *Server) createInfoWithRoutes() []byte {
	info := s.routeInfo
	for _, r := range s.routes {
		r.mu.Lock()

		var ipUrl string

		_, rport, err := net.SplitHostPort(r.route.url.Host)
		if err == nil {
			switch conn := r.nc.(type) {
			case *net.TCPConn, *tls.Conn:
				addr := conn.RemoteAddr().(*net.TCPAddr)
				hp := net.JoinHostPort(addr.IP.String(), rport)
				ipUrl = fmt.Sprintf("nats-route://%s/", hp)
			}
		}
		if ipUrl == "" {
			ipUrl = fmt.Sprintf("%s", r.route.url)
		}

		ri := RemoteInfo{
			RemoteID:     r.route.remoteID,
			URL:          fmt.Sprintf("%s", r.route.url),
			IP:           ipUrl,
			AuthRequired: r.route.authRequired,
			TLSRequired:  r.route.tlsRequired,
		}
		info.Routes = append(info.Routes, ri)
		r.mu.Unlock()
	}
	b, _ := json.Marshal(info)
	return []byte(fmt.Sprintf(InfoProto, b))
}

// Process the info message if we are a route.
func (c *client) processRouteInfo(info *Info) {
	defer func() {
		c.srv.mu.Lock()
		delete(c.srv.implPending, info.ID)
		c.srv.mu.Unlock()
	}()

	c.mu.Lock()
	// Connection can be closed at any time (by auth timeout, etc).
	// Does not make sense to continue here if connection is gone.
	if c.route == nil || c.nc == nil {
		c.mu.Unlock()
		return
	}

	// Remote server may have updated its list of routes.
	if c.route.updatedInfo {
		s := c.srv
		c.mu.Unlock()
		if len(info.Routes) > 0 {
			s.processImplicitRoutes(info.Routes)
		}

		// We are done.
		return
	}

	// This indicates that the next time we receive an INFO from this connection,
	// it will be an updated INFO with (possibly) new routes.
	c.route.updatedInfo = true

	// Need to set this for the detection of the route to self to work
	// in closeConnection().
	c.route.remoteID = info.ID

	// Detect route to self.
	if c.route.remoteID == c.srv.info.ID {
		c.mu.Unlock()
		c.closeConnection()
		return
	}

	// Copy over important information.
	c.route.authRequired = info.AuthRequired
	c.route.tlsRequired = info.TLSRequired

	// If we do not know this route's URL, construct one on the fly
	// from the information provided.
	if c.route.url == nil {
		// Add in the URL from host and port
		hp := net.JoinHostPort(info.Host, strconv.Itoa(info.Port))
		url, err := url.Parse(fmt.Sprintf("nats-route://%s/", hp))
		if err != nil {
			c.Errorf("Error parsing URL from INFO: %v\n", err)
			c.mu.Unlock()
			c.closeConnection()
			return
		}
		c.route.url = url
	}

	// Check to see if we have this remote already registered.
	// This can happen when both servers have routes to each other.
	s := c.srv
	c.mu.Unlock()

	if s.addRoute(c) {
		c.Debugf("Registering remote route %q", info.ID)

		var infoJSON []byte
		// Get the INFO protocol with routes details included
		s.mu.Lock()
		if len(s.routes) > 1 {
			infoJSON = s.createInfoWithRoutes()
		}
		s.mu.Unlock()

		if infoJSON != nil {
			// Send the INFO to the other side
			c.mu.Lock()
			c.sendInfo(infoJSON)
			c.mu.Unlock()
		}

		// Send our local subscriptions to this route.
		s.sendLocalSubsToRoute(c)
		if len(info.Routes) > 0 {
			s.processImplicitRoutes(info.Routes)
		}
	} else {
		c.Debugf("Detected duplicate remote route %q", info.ID)
		c.closeConnection()
	}
}

// This will process implicit route information from other servers.
// We will check to see if we have configured or are already connected,
// and if so we will ignore. Otherwise we will attempt to connect.
func (s *Server) processImplicitRoutes(routes []RemoteInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ri := range routes {
		// We may receive multiples INFO updates from different routes,
		// with repeating routes to try to connect to. But we need
		// to make sure we attempt to connect to a given route only once.
		// FIXME(ik): REVISIT this.
		if _, exists := s.implPending[ri.RemoteID]; exists {
			continue
		}
		// Check if this route already exists
		if _, exists := s.remotes[ri.RemoteID]; exists {
			continue
		}
		// Don't connect to ourself
		if ri.RemoteID == s.info.ID {
			continue
		}
		// Also, exclude a route if we have it as an explicit route
		isExplicit := false
		riUrl, err := url.Parse(ri.URL)
		if err != nil {
			Errorf("Error parsing URL '%v' from route '%v', skipping", ri.URL, ri.RemoteID)
			continue
		}
		for _, explicit := range s.opts.Routes {
			if strings.ToLower(riUrl.Host) == strings.ToLower(explicit.Host) {
				isExplicit = true
				break
			}
		}
		if isExplicit {
			continue
		}

		// We have a new route that we do not currently know about.
		// Process here and solicit a connection.
		r, err := url.Parse(ri.IP)
		if err != nil {
			Debugf("Error parsing URL from Remote INFO: %v\n", err)
			continue
		}
		if ri.AuthRequired {
			r.User = url.UserPassword(s.opts.ClusterUsername, s.opts.ClusterPassword)
		}

		// Kepp track that we are initiating a connect for this implicit route
		s.implPending[ri.RemoteID] = struct{}{}

		go s.connectToRoute(r, Implicit)
	}
}

// This will send local subscription state to a new route connection.
// FIXME(dlc) - This could be a DOS or perf issue with many clients
// and large subscription space. Plus buffering in place not a good idea.
func (s *Server) sendLocalSubsToRoute(route *client) {
	b := bytes.Buffer{}

	s.mu.Lock()
	for _, client := range s.clients {
		client.mu.Lock()
		subs := client.subs.All()
		client.mu.Unlock()
		for _, s := range subs {
			if sub, ok := s.(*subscription); ok {
				rsid := routeSid(sub)
				proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
				b.WriteString(proto)
			}
		}
	}
	s.mu.Unlock()

	route.mu.Lock()
	defer route.mu.Unlock()
	route.bw.Write(b.Bytes())
	route.bw.Flush()

	route.Debugf("Route sent local subscriptions")
}

func (s *Server) createRoute(conn net.Conn, rURL *url.URL, routeType RouteType) *client {
	didSolicit := rURL != nil
	r := &route{didSolicit: didSolicit, routeType: routeType}
	c := &client{srv: s, nc: conn, opts: clientOpts{}, typ: ROUTER, route: r}

	// Grab server variables
	s.mu.Lock()
	// get the INFO protocol with routes details
	infoJSON := s.createInfoWithRoutes()
	authRequired := s.routeInfo.AuthRequired
	tlsRequired := s.routeInfo.TLSRequired
	s.mu.Unlock()

	// Grab lock
	c.mu.Lock()

	// Initialize
	c.initClient(tlsRequired)

	c.Debugf("Route connection created")

	// Check for TLS
	if tlsRequired {
		// Copy off the config to add in ServerName if we
		tlsConfig := *s.opts.ClusterTLSConfig

		// If we solicited, we will act like the client, otherwise the server.
		if didSolicit {
			c.Debugf("Starting TLS route client handshake")
			// Specify the ServerName we are expecting.
			host, _, _ := net.SplitHostPort(rURL.Host)
			tlsConfig.ServerName = host
			c.nc = tls.Client(c.nc, &tlsConfig)
		} else {
			c.Debugf("Starting TLS route server handshake")
			c.nc = tls.Server(c.nc, &tlsConfig)
		}

		conn := c.nc.(*tls.Conn)

		// Setup the timeout
		ttl := secondsToDuration(s.opts.ClusterTLSTimeout)
		time.AfterFunc(ttl, func() { tlsTimeout(c, conn) })
		conn.SetReadDeadline(time.Now().Add(ttl))

		c.mu.Unlock()
		if err := conn.Handshake(); err != nil {
			c.Debugf("TLS route handshake error: %v", err)
			c.sendErr("Secure Connection - TLS Required")
			c.closeConnection()
			return nil
		}
		// Reset the read deadline
		conn.SetReadDeadline(time.Time{})

		// Re-Grab lock
		c.mu.Lock()

		// Verify that the connection did not go away while we released the lock.
		if c.nc == nil {
			c.mu.Unlock()
			return nil
		}

		// Rewrap bw
		c.bw = bufio.NewWriterSize(c.nc, s.opts.BufSize)

		// Do final client initialization

		// Set the Ping timer
		c.setPingTimer()

		// Spin up the read loop.
		go c.readLoop()

		c.Debugf("TLS handshake complete")
		cs := conn.ConnectionState()
		c.Debugf("TLS version %s, cipher suite %s", tlsVersion(cs.Version), tlsCipher(cs.CipherSuite))
	}

	// Queue Connect proto if we solicited the connection.
	if didSolicit {
		r.url = rURL
		c.Debugf("Route connect msg sent")
		c.sendConnect(tlsRequired)
	}

	// Send our info to the other side.
	c.sendInfo(infoJSON)

	// Check for Auth required state for incoming connections.
	if authRequired && !didSolicit {
		ttl := secondsToDuration(s.opts.ClusterAuthTimeout)
		c.setAuthTimer(ttl)
	}

	c.mu.Unlock()

	return c
}

const (
	_CRLF_  = "\r\n"
	_EMPTY_ = ""
	_SPC_   = " "
)

const (
	subProto   = "SUB %s %s %s" + _CRLF_
	unsubProto = "UNSUB %s%s" + _CRLF_
)

// FIXME(dlc) - Make these reserved and reject if they come in as a sid
// from a client connection.

const (
	RSID  = "RSID"
	QRSID = "QRSID"

	RSID_CID_INDEX   = 1
	RSID_SID_INDEX   = 2
	EXPECTED_MATCHES = 3
)

// FIXME(dlc) - This may be too slow, check at later date.
var qrsidRe = regexp.MustCompile(`QRSID:(\d+):([^\s]+)`)

func (s *Server) routeSidQueueSubscriber(rsid []byte) (*subscription, bool) {
	if !bytes.HasPrefix(rsid, []byte(QRSID)) {
		return nil, false
	}
	matches := qrsidRe.FindSubmatch(rsid)
	if matches == nil || len(matches) != EXPECTED_MATCHES {
		return nil, false
	}
	cid := uint64(parseInt64(matches[RSID_CID_INDEX]))

	s.mu.Lock()
	client := s.clients[cid]
	s.mu.Unlock()

	if client == nil {
		return nil, true
	}
	sid := matches[RSID_SID_INDEX]

	if sub, ok := (client.subs.Get(sid)).(*subscription); ok {
		return sub, true
	}
	return nil, true
}

func routeSid(sub *subscription) string {
	var qi string
	if len(sub.queue) > 0 {
		qi = "Q"
	}
	return fmt.Sprintf("%s%s:%d:%s", qi, RSID, sub.client.cid, sub.sid)
}

func (s *Server) addRoute(c *client) bool {
	id := c.route.remoteID
	s.mu.Lock()
	remote, exists := s.remotes[id]
	if !exists {
		s.routes[c.cid] = c
		s.remotes[id] = c
	}
	s.mu.Unlock()

	if exists && c.route.didSolicit {
		// upgrade to solicited?
		remote.mu.Lock()
		remote.route = c.route
		remote.mu.Unlock()
	}

	return !exists
}

func (s *Server) broadcastToRoutes(proto string) {
	var arg []byte
	if atomic.LoadInt32(&trace) == 1 {
		arg = []byte(proto[:len(proto)-LEN_CR_LF])
	}
	s.mu.Lock()
	for _, route := range s.routes {
		// FIXME(dlc) - Make same logic as deliverMsg
		route.mu.Lock()
		route.bw.WriteString(proto)
		route.bw.Flush()
		route.mu.Unlock()
		route.traceOutOp("", arg)
	}
	s.mu.Unlock()
}

// broadcastSubscribe will forward a client subscription
// to all active routes.
func (s *Server) broadcastSubscribe(sub *subscription) {
	if s.numRoutes() == 0 {
		return
	}
	rsid := routeSid(sub)
	proto := fmt.Sprintf(subProto, sub.subject, sub.queue, rsid)
	s.broadcastToRoutes(proto)
}

// broadcastUnSubscribe will forward a client unsubscribe
// action to all active routes.
func (s *Server) broadcastUnSubscribe(sub *subscription) {
	if s.numRoutes() == 0 {
		return
	}
	rsid := routeSid(sub)
	maxStr := _EMPTY_
	// Set max if we have it set and have not tripped auto-unsubscribe
	if sub.max > 0 && sub.nm < sub.max {
		maxStr = fmt.Sprintf(" %d", sub.max)
	}
	proto := fmt.Sprintf(unsubProto, rsid, maxStr)
	s.broadcastToRoutes(proto)
}

func (s *Server) routeAcceptLoop(ch chan struct{}) {
	hp := fmt.Sprintf("%s:%d", s.opts.ClusterHost, s.opts.ClusterPort)
	Noticef("Listening for route connections on %s", hp)
	l, e := net.Listen("tcp", hp)
	if e != nil {
		Fatalf("Error listening on router port: %d - %v", s.opts.Port, e)
		return
	}

	// Let them know we are up
	close(ch)

	// Setup state that can enable shutdown
	s.mu.Lock()
	s.routeListener = l
	s.mu.Unlock()

	tmpDelay := ACCEPT_MIN_SLEEP

	for s.isRunning() {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				Debugf("Temporary Route Accept Errorf(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else if s.isRunning() {
				Noticef("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		go s.createRoute(conn, nil, Implicit)
	}
	Debugf("Router accept loop exiting..")
	s.done <- true
}

// StartRouting will start the accept loop on the cluster host:port
// and will actively try to connect to listed routes.
func (s *Server) StartRouting() {
	// Check for TLSConfig
	tlsReq := s.opts.ClusterTLSConfig != nil
	info := Info{
		ID:           s.info.ID,
		Version:      s.info.Version,
		Host:         s.opts.ClusterHost,
		Port:         s.opts.ClusterPort,
		AuthRequired: false,
		TLSRequired:  tlsReq,
		SSLRequired:  tlsReq,
		TLSVerify:    tlsReq,
		MaxPayload:   s.info.MaxPayload,
	}
	// Check for Auth items
	if s.opts.ClusterUsername != "" {
		info.AuthRequired = true
	}
	s.routeInfo = info

	// Spin up the accept loop
	ch := make(chan struct{})
	go s.routeAcceptLoop(ch)
	<-ch

	// Solicit Routes if needed.
	s.solicitRoutes()
}

func (s *Server) reConnectToRoute(rUrl *url.URL) {
	time.Sleep(DEFAULT_ROUTE_RECONNECT)
	s.connectToRoute(rUrl, Explicit)
}

func (s *Server) connectToRoute(rUrl *url.URL, routeType RouteType) {
	for s.isRunning() && rUrl != nil {
		Debugf("Trying to connect to route on %s", rUrl.Host)
		conn, err := net.DialTimeout("tcp", rUrl.Host, DEFAULT_ROUTE_DIAL)
		if err != nil {
			Debugf("Error trying to connect to route: %v", err)
			select {
			case <-s.rcQuit:
				return
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				continue
			}
		}
		// We have a route connection here.
		// Go ahead and create it and exit this func.
		s.createRoute(conn, rUrl, routeType)
		return
	}
}

func (c *client) isSolicitedRoute() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.typ == ROUTER && c.route != nil && c.route.didSolicit
}

func (s *Server) solicitRoutes() {
	for _, r := range s.opts.Routes {
		go s.connectToRoute(r, Explicit)
	}
}

func (s *Server) numRoutes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.routes)
}
