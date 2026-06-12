package websocket

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func IsWebSocketUpgrade(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func Proxy(w http.ResponseWriter, r *http.Request, target string) error {
	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("failed to upgrade client connection: %w", err)
	}
	defer clientConn.Close()

	wsURL := strings.Replace(target, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)

	if r.URL.Path != "" {
		wsURL = wsURL + r.URL.Path
	}
	if r.URL.RawQuery != "" {
		wsURL = wsURL + "?" + r.URL.RawQuery
	}

	headers := http.Header{}
	for k, vv := range r.Header {
		if strings.EqualFold(k, "Upgrade") ||
			strings.EqualFold(k, "Connection") ||
			strings.EqualFold(k, "Sec-Websocket-Key") ||
			strings.EqualFold(k, "Sec-Websocket-Version") ||
			strings.EqualFold(k, "Sec-Websocket-Extensions") {
			continue
		}
		for _, v := range vv {
			headers.Add(k, v)
		}
	}

	backendConn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		return fmt.Errorf("failed to dial backend: %w", err)
	}
	defer backendConn.Close()

	done := make(chan struct{}, 2)

	go func() {
		defer func() { done <- struct{}{} }()
		pipeWS(clientConn, backendConn)
	}()

	go func() {
		defer func() { done <- struct{}{} }()
		pipeWS(backendConn, clientConn)
	}()

	<-done
	return nil
}

func pipeWS(dst, src *websocket.Conn) {
	for {
		msgType, msg, err := src.ReadMessage()
		if err != nil {
			if err == io.EOF || websocket.IsUnexpectedCloseError(err) {
				return
			}
			return
		}
		if err := dst.WriteMessage(msgType, msg); err != nil {
			return
		}
	}
}

func init() {
	log.SetOutput(io.Discard)
}
