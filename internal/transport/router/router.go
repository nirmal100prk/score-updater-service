package router

import (
	"score-updater-svc/internal/transport/websockets"

	"github.com/gin-gonic/gin"
)

func NewRouter(router *gin.Engine, wsHandler *websockets.WebSocketHandler) {

	router.GET("/ws", wsHandler.HandleWebSocket)

}
