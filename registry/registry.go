package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// PshawnRegistry is a service discovery and register center.
type PshawnRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

// ServerItem is a server instance.
type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath    = "/_pshawnrpc_/registry"
	defaultTimeout = time.Minute * 5
)

// New creates a registry instance with timeout setting.
func New(timeout time.Duration) *PshawnRegistry {
	return &PshawnRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultPshawnRegistry = New(defaultTimeout)

// putServer puts a new server or refreshes an existing server.
func (r *PshawnRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		s.start = time.Now()
	}
}

// aliveServers returns all alive servers.
func (r *PshawnRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

func (r *PshawnRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-PshawnRPC-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-PshawnRPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP registers an HTTP handler for PshawnRegistry messages on registryPath.
func (r *PshawnRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultPshawnRegistry.HandleHTTP(defaultPath)
}

func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// make sure there is enough time for a caller to send heartbeat before it's removed from registry
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heartbeat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-PshawnRPC-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc registry: heartbeat err:", err)
		return err
	}
	return nil
}
