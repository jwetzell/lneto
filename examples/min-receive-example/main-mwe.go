package main

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"syscall"
	"time"

	"github.com/soypat/lneto"
	"github.com/soypat/lneto/ethernet"
	"github.com/soypat/lneto/x/xnet"
	"go.bug.st/serial"
)

const pollTime = 5 * time.Millisecond
const protoTimeout = 5 * time.Second
const protoRetries = 3

const (
	tcpBufsize         = 2048
	tcpPacketQueueSize = 4
	// Number of connections in TCP pool.
	tcpConnPoolSize = 20
	// EstablishedTimeout sets the timeout for a TCP connection since it is acquired until it is established.
	// If the connection does not establish in this time it will be closed by the pool.
	tcpEstablishedTimeout = 4 * time.Second
	tcpCloseTimeout       = protoTimeout
)

var nanotime = func() int64 {
	return time.Now().UnixNano()
}

var network Interface

type Interface interface {
	SendEth(frame []byte) error
	RecvEth(dst []byte) (int, error)
	HardwareAddress6() ([6]byte, error)
	MaxFrameLength() (int, error)
}

type TCPInterface struct {
	conn *net.TCPConn
}

func (i *TCPInterface) SendEth(frame []byte) error {
	_, err := i.conn.Write(frame)
	return err
}

func (i *TCPInterface) RecvEth(dst []byte) (int, error) {
	return i.conn.Read(dst)
}

func (i *TCPInterface) HardwareAddress6() ([6]byte, error) {
	return [6]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x01}, nil
}

func (i *TCPInterface) MaxFrameLength() (int, error) {
	return 1500, nil
}

func NewTCPInterface(addr string) (*TCPInterface, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &TCPInterface{conn: conn.(*net.TCPConn)}, nil
}

type SerialInterface struct {
	port serial.Port
}

func NewSerialInterface(portName string, baudRate int) (*SerialInterface, error) {
	mode := &serial.Mode{
		BaudRate: baudRate,
	}
	port, err := serial.Open(portName, mode)
	if err != nil {
		return nil, err
	}
	return &SerialInterface{port: port}, nil
}

func (i *SerialInterface) SendEth(frame []byte) error {
	_, err := i.port.Write(frame)
	return err
}

func (i *SerialInterface) RecvEth(dst []byte) (int, error) {
	return i.port.Read(dst)
}

func (i *SerialInterface) HardwareAddress6() ([6]byte, error) {
	return [6]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x01}, nil
}

func (i *SerialInterface) MaxFrameLength() (int, error) {
	return 1500, nil
}

func main() {
	var stack xnet.StackAsync
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		iface, err := NewSerialInterface("/dev/cu.usbserial-B00214C8", 115200)
		if err != nil {
			fmt.Println("failed to create serial interface:", err)
			time.Sleep(1 * time.Second)
			continue
		}
		network = iface
		break
	}
	if network == nil {
		fmt.Println("failed to create serial interface after retries")
		os.Exit(1)
	}

	if err := run(ctx, &stack); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stack *xnet.StackAsync) error {
	hwaddr, err := network.HardwareAddress6()
	if err != nil {
		return err
	}
	framelen, err := network.MaxFrameLength()
	if err != nil {
		return err
	}
	err = stack.Reset(xnet.StackConfig{
		StaticAddress4: [4]byte{192,168,0,1},
		Hostname: "lneto-mwe",
		RandSeed: time.Now().UnixNano(),
		// A passive TCP listener to many remote ports takes up one spot, active TCP clients to one remote port take up a spot.
		MaxActiveTCPPorts: 1,
		// MaxUDPConns: 1 , // For MDNS support.
		// AcceptMulticast: true, // For MDNS.
		MTU:             uint16(framelen - ethernet.MaxOverheadSize),
		HardwareAddress: hwaddr,
	})
	if err != nil {
		return fmt.Errorf("configuring stack: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Start stack routine to leverage easy to use blocking+retrying APIs.
	// Other option is to instead use async API which leads to more verbose
	// and more stateful code.
	go stackLoop(ctx, stack)
	berkstack := stack.StackBlocking(stackBackoff).StackGo(xnet.StackGoConfig{
		ListenerPoolConfig: xnet.TCPPoolConfig{
			PoolSize:           tcpConnPoolSize,
			QueueSize:          tcpPacketQueueSize,
			TxBufSize:          tcpBufsize,
			RxBufSize:          tcpBufsize,
			NanoTime:           nanotime,
			EstablishedTimeout: tcpEstablishedTimeout,
			ClosingTimeout:     tcpCloseTimeout,
		},
	})

	laddr := net.TCPAddrFromAddrPort(netip.AddrPortFrom(netip.AddrFrom4([4]byte{192, 168, 0, 1}), 80))
	// raddr := net.TCPAddr{} // If active (client) connection then set raddr in which case a net.Conn type is returned.
	const sockstream = 0x1
	c, err := berkstack.Socket(ctx, "tcp", syscall.AF_INET, sockstream, laddr, nil)
	if err != nil {
		return fmt.Errorf("creating AF_INET stream socket: %w", err)
	}
	listener := c.(net.Listener)
	for ctx.Err() == nil {
		time.Sleep(pollTime)
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("conn failed:", err)
		}
		go handleConn(conn)
	}
	return nil
}

func handleConn(conn net.Conn) {
	// Always close conn on finishing work so connection is reused.
	defer conn.Close()

	// Do something with conn.
	conn.Write([]byte("Hello!"))
	fmt.Println("handled connection")
}

func stackLoop(ctx context.Context, stack *xnet.StackAsync) {
	// Enables logging of packets.
	var cap xnet.CapturePrinter
	must(cap.Configure(os.Stdout, xnet.CapturePrinterConfig{
		Now:           time.Now,
		TimePrecision: 3,
	}))
	frameLength, _ := network.MaxFrameLength()
	buf := make([]byte, frameLength)
	for ctx.Err() == nil {
		nwrite, err := stack.EgressEthernet(buf[:])
		if err != nil {
			fmt.Println("encaps err:", err)
		} else if nwrite > 0 {
			network.SendEth(buf[:nwrite])
			cap.PrintPacket("OUT", buf[:nwrite])
		}
		nread, err := network.RecvEth(buf[:])
		if err != nil {
			fmt.Println("network read err:", err)
		} else if nread > 0 {
			err = stack.IngressEthernet(buf[:nread])
			if err != nil && err != lneto.ErrPacketDrop {
				fmt.Println("demux err:", err)
			} else {
				cap.PrintPacket("IN ", buf[:nread])
			}
		}
		if nwrite == 0 && nread == 0 {
			time.Sleep(pollTime)
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func stackBackoff(consecutiveBackoffs uint) time.Duration {
	if consecutiveBackoffs < 10 {
		return time.Millisecond
	}
	return 10 * time.Millisecond
}
