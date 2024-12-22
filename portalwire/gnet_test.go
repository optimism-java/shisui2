package portalwire

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/log"
	"github.com/stretchr/testify/assert"
)

func TestGnetConn(t *testing.T) {
	glogger := log.NewGlogHandler(log.JSONHandler(os.Stderr))
	slogVerbosity := log.FromLegacyLevel(3)
	glogger.Verbosity(slogVerbosity)
	defaultLogger := log.NewLogger(glogger)
	log.SetDefault(defaultLogger)

	conn1 := NewGnetConn(log.New("gnet", "test1"))
	conn2 := NewGnetConn(log.New("gnet", "test2"))

	err := conn1.ListenUDP(context.Background(), ":9000")
	if err != nil {
		t.Fatal(err)
	}
	err = conn2.ListenUDP(context.Background(), ":9001")
	if err != nil {
		t.Fatal(err)
	}

	addr1 := conn1.localAddr.(*net.UDPAddr)
	addr2 := conn2.localAddr.(*net.UDPAddr)

	netipAddr1, err := udpAddrToAddrPort(addr1)
	if err != nil {
		t.Fatal(err)
	}

	netipAddr2, err := udpAddrToAddrPort(addr2)
	if err != nil {
		t.Fatal(err)
	}
	conn1.conn.WriteToUDPAddrPort([]byte("send from conn1"), netipAddr2)
	buf := make([]byte, 1280)
	n, _, err := conn2.ReadFromUDPAddrPort(buf)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, buf[:n], []byte("send from conn1"))

	conn2.conn.WriteToUDPAddrPort([]byte("send from conn2"), netipAddr1)
	buf = make([]byte, 1280)
	n, _, err = conn1.ReadFromUDPAddrPort(buf)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, buf[:n], []byte("send from conn2"))
}

func udpAddrToAddrPort(udpAddr *net.UDPAddr) (netip.AddrPort, error) {
	// Parse the IP to netip.Addr
	addr, err := netip.ParseAddr(udpAddr.IP.String())
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("failed to parse IP: %w", err)
	}

	// Combine the address and port
	return netip.AddrPortFrom(addr, uint16(udpAddr.Port)), nil
}
