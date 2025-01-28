package websockets

import (
	"log"
	"net/http"
	"score-updater-svc/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// WebSocketHandler handles WebSocket connections
type WebSocketHandler struct {
	messageService *service.MessageService
}

func NewWebSocketHandler(messageService *service.MessageService) *WebSocketHandler {
	return &WebSocketHandler{
		messageService: messageService,
	}
}

// HandleWebSocket upgrades the HTTP connection to a WebSocket connection
func (h *WebSocketHandler) HandleWebSocket(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Failed to upgrade WebSocket connection: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"message": "failed to initiate connection"})
		return
	}

	log.Println("Client connected")

	go h.handleConnection(conn)
}

func (h *WebSocketHandler) handleConnection(conn *websocket.Conn) {
	defer func() {
		conn.Close()
		log.Println("Client disconnected")
	}()

	messageChannel := make(chan string)

	// Start a goroutine to consume messages from Kafka
	go func() {
		for {
			message, err := h.messageService.StartConsuming()
			if err != nil {
				log.Printf("Failed to consume message from Kafka: %v", err)
				continue
			}
			messageChannel <- message
		}
	}()

	for {
		select {
		case message := <-messageChannel:
			// Push the Kafka message to the WebSocket client
			err := conn.WriteMessage(websocket.TextMessage, []byte(message))
			if err != nil {
				log.Printf("Failed to send message to WebSocket client: %v", err)
				return
			}
		}
	}
}
