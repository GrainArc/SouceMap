package routers

import (
	"github.com/GrainArc/SouceMap/GdalView"
	"github.com/gin-gonic/gin"
)

func GDALRouters(r *gin.Engine) {
	UserController := &GdalView.UserController{}
	mapRouter := r.Group("/gdal")
	{
		// POST用于提交分析任务配置
		mapRouter.POST("/Intersect/start", UserController.StartIntersect)
		// GET用于WebSocket连接
		mapRouter.GET("/Intersect/ws/:taskId", UserController.IntersectWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/Intersect/status/:taskId", UserController.GetTaskStatus)
	}
	{
		// POST用于提交对称差异分析任务配置
		mapRouter.POST("/SymDifference/start", UserController.StartSymDifference)
		// GET用于WebSocket连接
		mapRouter.GET("/SymDifference/ws/:taskId", UserController.SymDifferenceWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/SymDifference/status/:taskId", UserController.GetTaskStatus)
	}
	{
		// POST用于提交对称差异分析任务配置
		mapRouter.POST("/Union/start", UserController.StartUnion)
		// GET用于WebSocket连接
		mapRouter.GET("/Union/ws/:taskId", UserController.UnionWebSocket)
		// GET用于查询任务状态（可选）
		mapRouter.GET("/Union/status/:taskId", UserController.GetUnionTaskStatus)
	}
	{
		mapRouter.POST("/Clip/start", UserController.StartClip)
		mapRouter.GET("/Clip/ws/:taskId", UserController.ClipWebSocket)
		mapRouter.GET("/Clip/status/:taskId", UserController.GetClipTaskStatus)
	}
	{
		// POST用于提交擦除分析任务配置
		mapRouter.POST("/Erase/start", UserController.StartErase)
		// GET用于WebSocket连接
		mapRouter.GET("/Erase/ws/:taskId", UserController.EraseWebSocket)
		// GET用于查询任务状态
		mapRouter.GET("/Erase/status/:taskId", UserController.GetEraseTaskStatus)
	}
	{
		// Identity 相关路由
		mapRouter.POST("/Identity/start", UserController.StartIdentity)
		mapRouter.GET("/Identity/ws/:taskId", UserController.IdentityWebSocket)
		mapRouter.GET("/Identity/status/:taskId", UserController.GetIdentityTaskStatus)
	}
	{
		// 空间更新分析路由
		mapRouter.POST("/Update/start", UserController.StartUpdate)
		mapRouter.GET("/Update/ws/:taskId", UserController.UpdateWebSocket)
		mapRouter.GET("/Update/status/:taskId", UserController.GetUpdateTaskStatus)
	}
}
