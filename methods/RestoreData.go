package methods

import (
	"bufio"
	"database/sql"
	"fmt"
	"github.com/fmecool/SouceMap/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

//数据恢复

// RestoreConfig 恢复配置结构
type RestoreConfig struct {
	Host         string
	Port         string
	User         string
	Password     string
	Database     string
	BackupPath   string
	DropExisting bool // 是否删除现有数据
	RestoreData  bool // 是否恢复数据
	Verbose      bool // 详细日志
}

// SQLRestoreManager SQL恢复管理器
type SQLRestoreManager struct {
	config *RestoreConfig
	db     *gorm.DB
}

// RestoreResult 恢复结果
type RestoreResult struct {
	Success          bool
	TablesCreated    int
	DataRestored     int
	IndexesCreated   int
	ViewsCreated     int
	FunctionsCreated int
	Errors           []string
	Duration         time.Duration
}

// NewSQLRestoreManager 创建SQL恢复管理器
func NewSQLRestoreManager(backupPath string) (*SQLRestoreManager, error) {
	Mainconf := config.MainConfig
	config := &RestoreConfig{
		Host:         Mainconf.Host,
		Port:         Mainconf.Port,
		User:         Mainconf.Username,
		Password:     Mainconf.Password,
		Database:     Mainconf.Dbname,
		BackupPath:   backupPath,
		DropExisting: false, // 默认不删除现有数据
		RestoreData:  true,  // 默认恢复数据
		Verbose:      true,  // 默认详细日志
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		config.Host, config.User, config.Password, config.Database, config.Port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %v", err)
	}

	return &SQLRestoreManager{
		config: config,
		db:     db,
	}, nil
}

// PerformRestore 执行完整恢复
func (srm *SQLRestoreManager) PerformRestore() (*RestoreResult, error) {
	startTime := time.Now()
	result := &RestoreResult{
		Success: true,
		Errors:  make([]string, 0),
	}

	log.Printf("开始数据库恢复，备份路径: %s", srm.config.BackupPath)

	// 验证备份文件
	if err := srm.validateBackupFiles(); err != nil {
		return nil, fmt.Errorf("备份文件验证失败: %v", err)
	}

	// 1. 检查并启用PostGIS扩展
	if err := srm.enablePostGISExtension(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("启用PostGIS扩展失败: %v", err))
		log.Printf("警告: %s", result.Errors[len(result.Errors)-1])
	}

	// 2. 删除现有数据（如果需要）
	if srm.config.DropExisting {
		if err := srm.dropExistingData(); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("删除现有数据失败: %v", err))
		}
	}

	// 3. 恢复数据库结构（包含序列和表）
	if err := srm.restoreSchema(result); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("恢复数据库结构失败: %v", err))
		result.Success = false
		return result, err // 结构恢复失败直接返回
	}

	// 验证关键表是否存在
	if err := srm.verifyTablesCreated(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("验证表创建失败: %v", err))
		return result, err
	}

	// 4. 恢复表数据
	if srm.config.RestoreData {
		if err := srm.restoreTableData(result); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("恢复表数据失败: %v", err))
			result.Success = false
		}
	}

	// 5. 恢复约束和索引
	if err := srm.restoreConstraintsAndIndexes(result); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("恢复约束和索引失败: %v", err))
	}

	// 7. 更新序列当前值（基于实际数据）
	if err := srm.updateSequenceValues(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("更新序列值失败: %v", err))
	}

	// 8. 更新PostGIS几何列统计信息
	if err := srm.updateGeometryStatistics(); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("更新几何列统计信息失败: %v", err))
	}

	// 9. 验证恢复结果
	if err := srm.validateRestore(result); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("验证恢复结果失败: %v", err))
	}

	result.Duration = time.Since(startTime)

	if result.Success {
		log.Printf("数据库恢复完成，耗时: %v", result.Duration)
	} else {
		log.Printf("数据库恢复完成但有错误，耗时: %v", result.Duration)
	}

	return result, nil
}

// 新增：更新序列值的方法
func (srm *SQLRestoreManager) updateSequenceValues() error {
	log.Printf("更新序列当前值...")

	// 获取所有序列及其关联的表和列
	query := `
        SELECT 
            s.schemaname,
            s.sequencename,
            t.table_name,
            c.column_name
        FROM pg_sequences s
        JOIN information_schema.columns c ON c.column_default LIKE '%' || s.sequencename || '%'
        JOIN information_schema.tables t ON t.table_name = c.table_name
        WHERE s.schemaname = 'public' 
        AND t.table_schema = 'public'
        AND c.table_schema = 'public'
    `

	var seqInfos []struct {
		SchemaName   string `gorm:"column:schemaname"`
		SequenceName string `gorm:"column:sequencename"`
		TableName    string `gorm:"column:table_name"`
		ColumnName   string `gorm:"column:column_name"`
	}

	if err := srm.db.Raw(query).Scan(&seqInfos).Error; err != nil {
		log.Printf("获取序列信息失败: %v", err)
		return err
	}

	for _, seqInfo := range seqInfos {
		// 获取表中该列的最大值
		var maxValue sql.NullInt64
		maxQuery := fmt.Sprintf("SELECT MAX(%s) FROM %s", seqInfo.ColumnName, seqInfo.TableName)

		if err := srm.db.Raw(maxQuery).Scan(&maxValue).Error; err != nil {
			log.Printf("获取表%s列%s最大值失败: %v", seqInfo.TableName, seqInfo.ColumnName, err)
			continue
		}

		if maxValue.Valid && maxValue.Int64 > 0 {
			// 设置序列值为最大值+1
			setValueSQL := fmt.Sprintf("SELECT setval('%s', %d, true)",
				seqInfo.SequenceName, maxValue.Int64)

			if err := srm.db.Exec(setValueSQL).Error; err != nil {
				log.Printf("设置序列%s值失败: %v", seqInfo.SequenceName, err)
			} else {
				log.Printf("✓ 序列%s值已更新为%d", seqInfo.SequenceName, maxValue.Int64)
			}
		}
	}

	return nil
}

// validateBackupFiles 验证备份文件
func (srm *SQLRestoreManager) validateBackupFiles() error {
	requiredFiles := []string{
		"schema.sql",
		"data",
		"constraints.sql",

		"restore.sql",
	}

	for _, file := range requiredFiles {
		path := filepath.Join(srm.config.BackupPath, file)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return fmt.Errorf("必需的备份文件不存在: %s", file)
		}
	}

	return nil
}

// enablePostGISExtension 启用PostGIS扩展
func (srm *SQLRestoreManager) enablePostGISExtension() error {
	log.Printf("检查并启用PostGIS扩展...")

	extensions := []string{
		"CREATE EXTENSION IF NOT EXISTS postgis;",
		"CREATE EXTENSION IF NOT EXISTS postgis_topology;",
		"CREATE EXTENSION IF NOT EXISTS postgis_raster;",
		"CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;",
		"CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;",
	}

	for _, ext := range extensions {
		if err := srm.db.Exec(ext).Error; err != nil {
			log.Printf("执行扩展语句失败 %s: %v", ext, err)
			// 某些扩展可能不可用，继续执行
		}
	}

	return nil
}

// dropExistingData 删除现有数据
func (srm *SQLRestoreManager) dropExistingData() error {
	log.Printf("删除现有数据...")

	// 1. 删除序列
	var sequences []string
	seqQuery := `SELECT sequencename FROM pg_sequences WHERE schemaname = 'public'`
	if err := srm.db.Raw(seqQuery).Pluck("sequencename", &sequences).Error; err != nil {
		log.Printf("获取序列列表失败: %v", err)
	} else {
		for _, seq := range sequences {
			dropSeqSQL := fmt.Sprintf("DROP SEQUENCE IF EXISTS %s CASCADE;", seq)
			if err := srm.db.Exec(dropSeqSQL).Error; err != nil {
				log.Printf("删除序列%s失败: %v", seq, err)
			}
		}
	}

	// 2. 删除表（现有逻辑）
	var tables []string
	query := `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        AND table_name NOT LIKE 'spatial_%'
        AND table_name NOT LIKE 'geography_%'
        AND table_name NOT LIKE 'geometry_%'
    `

	if err := srm.db.Raw(query).Pluck("table_name", &tables).Error; err != nil {
		return err
	}

	if err := srm.db.Exec("SET session_replication_role = replica;").Error; err != nil {
		return err
	}

	for _, table := range tables {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", table)
		if err := srm.db.Exec(dropSQL).Error; err != nil {
			log.Printf("删除表%s失败: %v", table, err)
		}
	}

	if err := srm.db.Exec("SET session_replication_role = DEFAULT;").Error; err != nil {
		return err
	}

	return nil
}

// restoreSchema 恢复数据库结构
func (srm *SQLRestoreManager) restoreSchema(result *RestoreResult) error {
	log.Printf("恢复数据库结构...")

	schemaFile := filepath.Join(srm.config.BackupPath, "schema.sql")
	return srm.executeSQLFile(schemaFile, &result.TablesCreated)
}

// restoreTableDataFileChunked 分块读取大文件
func (srm *SQLRestoreManager) restoreTableDataFileChunked(filePath, tableName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 检查是否是几何表
	isGeometryTable, err := srm.isGeometryTable(tableName)
	if err != nil {
		log.Printf("检查几何表失败: %v", err)
	}

	const bufferSize = 1024 * 1024 // 1MB buffer
	reader := bufio.NewReaderSize(file, bufferSize)

	var sqlBuffer strings.Builder
	var lineBuffer strings.Builder
	lineCount := 0

	for {
		chunk, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return fmt.Errorf("读取文件失败: %v", err)
		}

		if len(chunk) > 0 {
			lineBuffer.Write(chunk)

			// 如果读到完整行（以换行符结尾）
			if chunk[len(chunk)-1] == '\n' {
				line := strings.TrimSpace(lineBuffer.String())
				lineBuffer.Reset()

				// 跳过注释和空行
				if line == "" || strings.HasPrefix(line, "--") {
					continue
				}

				// 处理几何数据
				if isGeometryTable {
					line = srm.processGeometryData(line)
				}

				sqlBuffer.WriteString(line)

				// 如果行以分号结尾，执行SQL
				if strings.HasSuffix(line, ";") {
					sql := sqlBuffer.String()
					if err := srm.db.Exec(sql).Error; err != nil {
						log.Printf("执行SQL失败 (行 %d): %v", lineCount, err)
					}
					sqlBuffer.Reset()
					lineCount++

					if lineCount%1000 == 0 {
						log.Printf("已处理 %d 条记录", lineCount)
					}
				} else {
					sqlBuffer.WriteString(" ")
				}
			}
		}

		if err == io.EOF {
			break
		}
	}

	// 处理最后的SQL
	if sqlBuffer.Len() > 0 {
		sql := sqlBuffer.String()
		if err := srm.db.Exec(sql).Error; err != nil {
			log.Printf("执行最后的SQL失败: %v", err)
		}
	}

	log.Printf("表 %s 数据恢复完成，共处理 %d 条记录", tableName, lineCount)
	return nil
}

// restoreTableData 恢复表数据
func (srm *SQLRestoreManager) restoreTableData(result *RestoreResult) error {
	log.Printf("恢复表数据...")

	dataDir := filepath.Join(srm.config.BackupPath, "data")
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	// 禁用外键检查和触发器
	if err := srm.db.Exec("SET session_replication_role = replica;").Error; err != nil {
		return err
	}
	defer srm.db.Exec("SET session_replication_role = DEFAULT;")

	for _, file := range files {
		if strings.HasSuffix(file.Name(), "_data.sql") {
			tableName := strings.TrimSuffix(file.Name(), "_data.sql")
			log.Printf("恢复表数据: %s", tableName)

			filePath := filepath.Join(dataDir, file.Name())
			if err := srm.restoreTableDataFile(filePath, tableName); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("恢复表%s数据失败: %v", tableName, err))
			} else {
				result.DataRestored++
			}
		}
	}

	return nil
}

// restoreTableDataFile 恢复单个表数据文件
func (srm *SQLRestoreManager) restoreTableDataFile(filePath, tableName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// 创建Scanner并设置更大的缓冲区
	scanner := bufio.NewScanner(file)

	// 设置更大的缓冲区大小 (默认64KB -> 10MB)
	const maxCapacity = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	var sqlBuffer strings.Builder
	lineCount := 0

	// 检查是否是几何表
	isGeometryTable, err := srm.isGeometryTable(tableName)
	if err != nil {
		log.Printf("检查几何表失败: %v", err)
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// 跳过注释和空行
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		// 处理几何数据
		if isGeometryTable {
			line = srm.processGeometryData(line)
		}

		sqlBuffer.WriteString(line)

		// 如果行以分号结尾，执行SQL
		if strings.HasSuffix(line, ";") {
			sql := sqlBuffer.String()
			if err := srm.db.Exec(sql).Error; err != nil {
				log.Printf("执行SQL失败 (行 %d): %v\nSQL: %s", lineCount, err, sql[:min(200, len(sql))])
				// 继续执行其他语句
			}
			sqlBuffer.Reset()
			lineCount++

			// 每1000条记录提交一次
			if lineCount%1000 == 0 {
				log.Printf("已处理 %d 条记录", lineCount)
			}
		} else {
			sqlBuffer.WriteString(" ")
		}
	}

	// 执行剩余的SQL
	if sqlBuffer.Len() > 0 {
		sql := sqlBuffer.String()
		if err := srm.db.Exec(sql).Error; err != nil {
			log.Printf("执行最后的SQL失败: %v", err)
		}
	}

	// 检查Scanner错误
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("读取文件失败: %v", err)
	}

	log.Printf("表 %s 数据恢复完成，共处理 %d 条记录", tableName, lineCount)
	return nil
}

// isGeometryTable 检查是否是几何表
func (srm *SQLRestoreManager) isGeometryTable(tableName string) (bool, error) {
	var count int64
	query := `
		SELECT COUNT(*) 
		FROM information_schema.columns 
		WHERE table_name = ? 
		AND table_schema = 'public'
		AND (data_type = 'USER-DEFINED' 
		     OR udt_name IN ('geometry', 'geography', 'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon', 'geometrycollection'))
	`

	if err := srm.db.Raw(query, tableName).Scan(&count).Error; err != nil {
		return false, err
	}

	return count > 0, nil
}

// processGeometryData 处理几何数据
func (srm *SQLRestoreManager) processGeometryData(line string) string {
	// 处理几何数据的特殊格式
	// PostGIS的几何数据通常以ST_GeomFromText或ST_GeomFromWKB格式存储

	// 查找并替换几何数据格式
	geomPattern := regexp.MustCompile(`'([^']*POINT[^']*|[^']*LINESTRING[^']*|[^']*POLYGON[^']*|[^']*MULTIPOINT[^']*|[^']*MULTILINESTRING[^']*|[^']*MULTIPOLYGON[^']*|[^']*GEOMETRYCOLLECTION[^']*)'`)

	line = geomPattern.ReplaceAllStringFunc(line, func(match string) string {
		// 移除外层引号
		wkt := strings.Trim(match, "'")
		// 使用ST_GeomFromText函数
		return fmt.Sprintf("ST_GeomFromText('%s')", wkt)
	})

	// 处理二进制几何数据
	hexPattern := regexp.MustCompile(`'\\x([0-9A-Fa-f]+)'`)
	line = hexPattern.ReplaceAllStringFunc(line, func(match string) string {
		hex := strings.TrimPrefix(strings.Trim(match, "'"), "\\x")
		return fmt.Sprintf("ST_GeomFromWKB('\\x%s')", hex)
	})

	return line
}

// restoreConstraintsAndIndexes 恢复约束和索引
func (srm *SQLRestoreManager) restoreConstraintsAndIndexes(result *RestoreResult) error {
	log.Printf("恢复约束和索引...")

	constraintsFile := filepath.Join(srm.config.BackupPath, "constraints.sql")
	return srm.executeSQLFile(constraintsFile, &result.IndexesCreated)
}

// executeSQLFile 执行SQL文件
func (srm *SQLRestoreManager) executeSQLFile(filePath string, counter *int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var sqlBuffer strings.Builder
	lineNumber := 0
	successCount := 0
	errorCount := 0

	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())

		// 跳过注释和空行
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		sqlBuffer.WriteString(line)
		sqlBuffer.WriteString(" ")

		// 如果行以分号结尾，执行SQL
		if strings.HasSuffix(line, ";") {
			sql := strings.TrimSpace(sqlBuffer.String())
			if sql != "" {
				if err := srm.db.Exec(sql).Error; err != nil {
					errorCount++
					log.Printf("执行SQL失败 (文件: %s, 行: %d): %v",
						filepath.Base(filePath), lineNumber, err)
					log.Printf("失败的SQL: %s", sql[:min(200, len(sql))])

					// 对于schema.sql，如果是CREATE TABLE失败，这是严重错误
					if strings.Contains(filepath.Base(filePath), "schema") &&
						strings.Contains(strings.ToUpper(sql), "CREATE TABLE") {
						return fmt.Errorf("创建表失败: %v", err)
					}
				} else {
					successCount++
					*counter++
					if srm.config.Verbose && strings.Contains(strings.ToUpper(sql), "CREATE TABLE") {
						log.Printf("✓ 创建表成功: %s", sql[:min(100, len(sql))])
					}
				}
			}
			sqlBuffer.Reset()
		}
	}

	log.Printf("文件 %s 执行完成: 成功 %d, 失败 %d",
		filepath.Base(filePath), successCount, errorCount)

	return scanner.Err()
}

// updateGeometryStatistics 更新几何列统计信息
func (srm *SQLRestoreManager) updateGeometryStatistics() error {
	log.Printf("更新PostGIS几何列统计信息...")

	// 获取所有几何列
	query := `
		SELECT f_table_name, f_geometry_column, srid, type
		FROM geometry_columns
		WHERE f_table_schema = 'public'
	`

	var geometryColumns []struct {
		TableName  string `gorm:"column:f_table_name"`
		GeomColumn string `gorm:"column:f_geometry_column"`
		SRID       int    `gorm:"column:srid"`
		GeomType   string `gorm:"column:type"`
	}

	if err := srm.db.Raw(query).Scan(&geometryColumns).Error; err != nil {
		// 如果geometry_columns表不存在，尝试从information_schema获取
		log.Printf("从geometry_columns获取信息失败，尝试其他方法: %v", err)
		return srm.updateGeometryStatisticsAlternative()
	}

	for _, geomCol := range geometryColumns {
		// 更新几何列的边界框统计
		updateSQL := fmt.Sprintf("SELECT UpdateGeometrySRID('%s', '%s', %d);",
			geomCol.TableName, geomCol.GeomColumn, geomCol.SRID)

		if err := srm.db.Exec(updateSQL).Error; err != nil {
			log.Printf("更新表%s列%s的SRID失败: %v", geomCol.TableName, geomCol.GeomColumn, err)
		}

		// 分析表统计信息
		analyzeSQL := fmt.Sprintf("ANALYZE %s;", geomCol.TableName)
		if err := srm.db.Exec(analyzeSQL).Error; err != nil {
			log.Printf("分析表%s失败: %v", geomCol.TableName, err)
		}
	}

	return nil
}

// updateGeometryStatisticsAlternative 替代方法更新几何统计
func (srm *SQLRestoreManager) updateGeometryStatisticsAlternative() error {
	// 获取包含几何列的表
	query := `
		SELECT DISTINCT table_name
		FROM information_schema.columns 
		WHERE table_schema = 'public'
		AND (data_type = 'USER-DEFINED' 
		     OR udt_name IN ('geometry', 'geography'))
	`

	var tables []string
	if err := srm.db.Raw(query).Pluck("table_name", &tables).Error; err != nil {
		return err
	}

	for _, table := range tables {
		analyzeSQL := fmt.Sprintf("ANALYZE %s;", table)
		if err := srm.db.Exec(analyzeSQL).Error; err != nil {
			log.Printf("分析表%s失败: %v", table, err)
		}
	}

	return nil
}

// validateRestore 验证恢复结果
func (srm *SQLRestoreManager) validateRestore(result *RestoreResult) error {
	log.Printf("验证恢复结果...")

	// 检查表数量
	var tableCount int64
	if err := srm.db.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'").Scan(&tableCount).Error; err != nil {
		return err
	}

	log.Printf("恢复完成统计:")
	log.Printf("- 创建表: %d", result.TablesCreated)
	log.Printf("- 恢复数据: %d 个表", result.DataRestored)
	log.Printf("- 创建索引: %d", result.IndexesCreated)

	log.Printf("- 数据库中总表数: %d", tableCount)
	log.Printf("- 错误数量: %d", len(result.Errors))

	if len(result.Errors) > 0 {
		log.Printf("错误详情:")
		for i, err := range result.Errors {
			log.Printf("  %d. %s", i+1, err)
		}
	}

	return nil
}

// SetRestoreOptions 设置恢复选项
func (srm *SQLRestoreManager) SetRestoreOptions(dropExisting, restoreData, verbose bool) {
	srm.config.DropExisting = dropExisting
	srm.config.RestoreData = restoreData
	srm.config.Verbose = verbose
}

// min 辅助函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func (srm *SQLRestoreManager) verifyTablesCreated() error {
	log.Printf("验证表是否创建成功...")

	var tables []string
	query := `
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    `

	if err := srm.db.Raw(query).Pluck("table_name", &tables).Error; err != nil {
		return fmt.Errorf("查询表列表失败: %v", err)
	}

	log.Printf("当前数据库中的表 (%d个):", len(tables))
	for _, table := range tables {
		log.Printf("  - %s", table)
	}

	return nil
}

// 在 restoreTableData 之前添加
func (srm *SQLRestoreManager) ensureTableExists(tableName string) error {
	var exists bool
	query := `
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = ?
        )
    `

	if err := srm.db.Raw(query, tableName).Scan(&exists).Error; err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("表 %s 不存在，需要先创建", tableName)
	}

	return nil
}

func RestoreData(backupPath string) {
	// 创建备份管理器

	restoreManager, err := NewSQLRestoreManager(backupPath)
	if err != nil {
		log.Fatal("创建恢复管理器失败:", err)
	}

	// 设置恢复选项
	restoreManager.SetRestoreOptions(
		true, // 不删除现有数据
		true, // 恢复数据
		true, // 详细日志
	)

	// 执行恢复
	result, err := restoreManager.PerformRestore()
	if err != nil {
		log.Fatal("恢复失败:", err)
	}

	// 输出结果
	if result.Success {
		log.Printf("恢复成功！耗时: %v", result.Duration)
	} else {
		log.Printf("恢复完成但有错误，耗时: %v", result.Duration)
		log.Printf("错误数量: %d", len(result.Errors))
	}
}
