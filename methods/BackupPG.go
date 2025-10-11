package methods

import (
	"database/sql"
	"github.com/GrainArc/SouceMap/config"

	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// BackupConfig 备份配置结构
type BackupConfig struct {
	Host          string
	Port          string
	User          string
	Password      string
	Database      string
	BackupDir     string
	RetentionDays int
	Compress      bool
}

// SQLBackupManager SQL备份管理器
type SQLBackupManager struct {
	config *BackupConfig
	db     *gorm.DB
}

// NewSQLBackupManager 创建SQL备份管理器
func NewSQLBackupManager() (*SQLBackupManager, error) {
	Mainconf := config.MainConfig
	config := BackupConfig{
		Host:          Mainconf.Host,
		Port:          Mainconf.Port,
		User:          Mainconf.Username,
		Password:      Mainconf.Password,
		Database:      Mainconf.Dbname,
		BackupDir:     Mainconf.Download,
		RetentionDays: 14,
		Compress:      true,
	}
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=disable",
		config.Host, config.User, config.Password, config.Database, config.Port)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %v", err)
	}

	return &SQLBackupManager{
		config: &config,
		db:     db,
	}, nil
}

// TableInfo 表信息结构
type TableInfo struct {
	TableName  string `gorm:"column:table_name"`
	SchemaName string `gorm:"column:table_schema"`
	TableType  string `gorm:"column:table_type"`
	RowCount   int64  `gorm:"column:row_count"`
}

// ColumnInfo 列信息结构
type ColumnInfo struct {
	ColumnName    string `gorm:"column:column_name"`
	DataType      string `gorm:"column:data_type"`
	IsNullable    string `gorm:"column:is_nullable"`
	ColumnDefault string `gorm:"column:column_default"`
	CharMaxLength int    `gorm:"column:character_maximum_length"`
}

// PerformSQLBackup 执行SQL备份
func (sbm *SQLBackupManager) PerformSQLBackup() error {
	log.Printf("开始执行SQL备份...")

	// 创建备份目录
	timestamp := time.Now().Format("20060102_150405")
	backupDir := filepath.Join(sbm.config.BackupDir, fmt.Sprintf("%s_backup_%s", sbm.config.Database, timestamp))

	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("创建备份目录失败: %v", err)
	}

	// 1. 备份数据库结构
	if err := sbm.backupSchema(backupDir); err != nil {
		return fmt.Errorf("备份数据库结构失败: %v", err)
	}

	// 2. 备份表数据
	if err := sbm.backupTableData(backupDir); err != nil {
		return fmt.Errorf("备份表数据失败: %v", err)
	}

	// 3. 备份索引和约束
	if err := sbm.backupIndexesAndConstraints(backupDir); err != nil {
		return fmt.Errorf("备份索引和约束失败: %v", err)
	}

	// 5. 创建恢复脚本
	if err := sbm.createRestoreScript(backupDir); err != nil {
		return fmt.Errorf("创建恢复脚本失败: %v", err)
	}

	log.Printf("SQL备份完成: %s", backupDir)

	// 新增：清理旧备份
	if sbm.config.RetentionDays > 0 {
		if err := sbm.cleanupOldBackups(); err != nil {
			log.Printf("清理旧备份失败: %v", err)
			// 清理失败不影响备份成功
		}
	}
	return nil
}
func (sbm *SQLBackupManager) cleanupOldBackups() error {
	log.Printf("开始清理超过 %d 天的旧备份...", sbm.config.RetentionDays)

	// 获取备份目录中的所有备份文件夹
	backupDirs, err := sbm.getBackupDirectories()
	if err != nil {
		return fmt.Errorf("获取备份目录失败: %v", err)
	}

	if len(backupDirs) == 0 {
		log.Printf("没有找到备份目录")
		return nil
	}

	// 计算截止时间
	cutoffTime := time.Now().AddDate(0, 0, -sbm.config.RetentionDays)

	deletedCount := 0
	var totalSize int64

	for _, backupInfo := range backupDirs {
		if backupInfo.CreatedTime.Before(cutoffTime) {
			// 计算目录大小
			size, err := sbm.getDirSize(backupInfo.Path)
			if err != nil {
				log.Printf("计算目录大小失败 %s: %v", backupInfo.Path, err)
			} else {
				totalSize += size
			}

			// 删除旧备份
			if err := os.RemoveAll(backupInfo.Path); err != nil {
				log.Printf("删除旧备份失败 %s: %v", backupInfo.Path, err)
				continue
			}

			log.Printf("已删除旧备份: %s (创建时间: %s, 大小: %.2f MB)",
				backupInfo.Name,
				backupInfo.CreatedTime.Format("2006-01-02 15:04:05"),
				float64(size)/(1024*1024))
			deletedCount++
		}
	}

	if deletedCount > 0 {
		log.Printf("清理完成: 删除了 %d 个旧备份，释放空间 %.2f MB",
			deletedCount, float64(totalSize)/(1024*1024))
	} else {
		log.Printf("没有需要清理的旧备份")
	}

	return nil
}

// BackupInfo 备份信息结构
type BackupInfo struct {
	Name        string
	Path        string
	CreatedTime time.Time
	Size        int64
}

// getBackupDirectories 获取所有备份目录
func (sbm *SQLBackupManager) getBackupDirectories() ([]BackupInfo, error) {
	entries, err := os.ReadDir(sbm.config.BackupDir)
	if err != nil {
		return nil, err
	}

	var backupDirs []BackupInfo
	// 匹配备份目录名称的正则表达式 (database_backup_20240101_150405)
	backupPattern := regexp.MustCompile(fmt.Sprintf(`^%s_backup_(\d{8}_\d{6})$`,
		regexp.QuoteMeta(sbm.config.Database)))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := backupPattern.FindStringSubmatch(entry.Name())
		if len(matches) != 2 {
			continue
		}

		// 解析时间戳
		timestamp := matches[1]
		createdTime, err := time.Parse("20060102_150405", timestamp)
		if err != nil {
			log.Printf("解析备份时间戳失败 %s: %v", timestamp, err)
			continue
		}

		backupPath := filepath.Join(sbm.config.BackupDir, entry.Name())

		// 获取目录大小
		size, err := sbm.getDirSize(backupPath)
		if err != nil {
			log.Printf("获取目录大小失败 %s: %v", backupPath, err)
			size = 0
		}

		backupDirs = append(backupDirs, BackupInfo{
			Name:        entry.Name(),
			Path:        backupPath,
			CreatedTime: createdTime,
			Size:        size,
		})
	}

	// 按创建时间排序
	sort.Slice(backupDirs, func(i, j int) bool {
		return backupDirs[i].CreatedTime.Before(backupDirs[j].CreatedTime)
	})

	return backupDirs, nil
}

// getDirSize 计算目录大小
func (sbm *SQLBackupManager) getDirSize(path string) (int64, error) {
	var size int64

	err := filepath.Walk(path, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}

// GetBackupStatistics 获取备份统计信息
func (sbm *SQLBackupManager) GetBackupStatistics() (*BackupStatistics, error) {
	backupDirs, err := sbm.getBackupDirectories()
	if err != nil {
		return nil, err
	}

	stats := &BackupStatistics{
		TotalBackups:  len(backupDirs),
		RetentionDays: sbm.config.RetentionDays,
	}

	cutoffTime := time.Now().AddDate(0, 0, -sbm.config.RetentionDays)

	for _, backup := range backupDirs {
		stats.TotalSize += backup.Size

		if backup.CreatedTime.After(cutoffTime) {
			stats.ValidBackups++
			stats.ValidSize += backup.Size
		} else {
			stats.ExpiredBackups++
			stats.ExpiredSize += backup.Size
		}
	}

	if len(backupDirs) > 0 {
		stats.OldestBackup = backupDirs[0].CreatedTime
		stats.NewestBackup = backupDirs[len(backupDirs)-1].CreatedTime
	}

	return stats, nil
}

// BackupStatistics 备份统计信息
type BackupStatistics struct {
	TotalBackups   int       `json:"total_backups"`
	ValidBackups   int       `json:"valid_backups"`
	ExpiredBackups int       `json:"expired_backups"`
	TotalSize      int64     `json:"total_size"`
	ValidSize      int64     `json:"valid_size"`
	ExpiredSize    int64     `json:"expired_size"`
	RetentionDays  int       `json:"retention_days"`
	OldestBackup   time.Time `json:"oldest_backup"`
	NewestBackup   time.Time `json:"newest_backup"`
}

// 格式化统计信息
func (bs *BackupStatistics) String() string {
	return fmt.Sprintf(`备份统计信息:
- 总备份数: %d
- 有效备份数: %d (保留期内)
- 过期备份数: %d
- 总大小: %.2f MB
- 有效备份大小: %.2f MB
- 过期备份大小: %.2f MB
- 保留天数: %d
- 最旧备份: %s
- 最新备份: %s`,
		bs.TotalBackups,
		bs.ValidBackups,
		bs.ExpiredBackups,
		float64(bs.TotalSize)/(1024*1024),
		float64(bs.ValidSize)/(1024*1024),
		float64(bs.ExpiredSize)/(1024*1024),
		bs.RetentionDays,
		bs.OldestBackup.Format("2006-01-02 15:04:05"),
		bs.NewestBackup.Format("2006-01-02 15:04:05"))
}

// backupSchema 备份数据库结构
// 在 backupSchema 方法中添加序列备份
func (sbm *SQLBackupManager) backupSchema(backupDir string) error {
	log.Printf("备份数据库结构...")

	schemaFile := filepath.Join(backupDir, "schema.sql")
	file, err := os.Create(schemaFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入文件头
	file.WriteString(fmt.Sprintf("-- PostgreSQL Database Schema Backup\n"))
	file.WriteString(fmt.Sprintf("-- Database: %s\n", sbm.config.Database))
	file.WriteString(fmt.Sprintf("-- Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	// 1. 首先备份序列
	if err := sbm.backupSequences(file); err != nil {
		log.Printf("备份序列失败: %v", err)
	}

	// 2. 然后备份表
	tables, err := sbm.getAllTables()
	if err != nil {
		return err
	}

	for _, table := range tables {
		if table.TableType == "BASE TABLE" {
			createSQL, err := sbm.getCreateTableSQL(table.SchemaName, table.TableName)
			if err != nil {
				log.Printf("获取表%s的创建语句失败: %v", table.TableName, err)
				continue
			}
			file.WriteString(fmt.Sprintf("-- Table: %s\n", table.TableName))
			file.WriteString(createSQL + "\n\n")
		}
	}

	return nil
}

// 简化版本的序列备份
func (sbm *SQLBackupManager) backupSequences(file *os.File) error {
	log.Printf("备份序列...")

	// 获取所有序列名称
	var sequenceNames []string
	query := `
        SELECT c.relname as sequence_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
        AND n.nspname = 'public'
        ORDER BY c.relname
    `

	if err := sbm.db.Raw(query).Pluck("sequence_name", &sequenceNames).Error; err != nil {
		log.Printf("获取序列名称失败: %v", err)
		return err
	}

	if len(sequenceNames) == 0 {
		log.Printf("没有找到序列")
		return nil
	}

	file.WriteString("-- Sequences\n")
	log.Printf("找到 %d 个序列", len(sequenceNames))

	for _, seqName := range sequenceNames {
		log.Printf("备份序列: %s", seqName)

		// 简单创建序列
		createSeqSQL := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s;\n", seqName)
		file.WriteString(createSeqSQL)

		// 尝试获取并设置当前值
		var lastValue int64
		valueQuery := fmt.Sprintf("SELECT last_value FROM %s", seqName)
		if err := sbm.db.Raw(valueQuery).Scan(&lastValue).Error; err != nil {
			log.Printf("获取序列 %s 当前值失败: %v", seqName, err)
		} else if lastValue > 1 {
			setValueSQL := fmt.Sprintf("SELECT setval('%s', %d, true);\n", seqName, lastValue)
			file.WriteString(setValueSQL)
			log.Printf("序列 %s 当前值: %d", seqName, lastValue)
		}

		file.WriteString("\n")
	}

	file.WriteString("\n")
	return nil
}

// getAllTables 获取所有表
func (sbm *SQLBackupManager) getAllTables() ([]TableInfo, error) {
	var tables []TableInfo

	query := `
        SELECT 
            t.table_name,
            t.table_schema,
            t.table_type,
            COALESCE(s.n_tup_ins + s.n_tup_upd + s.n_tup_del, 0) as row_count
        FROM information_schema.tables t
        LEFT JOIN pg_stat_user_tables s ON t.table_name = s.relname
        WHERE t.table_schema = 'public' 
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_name
    `

	if err := sbm.db.Raw(query).Scan(&tables).Error; err != nil {
		return nil, err
	}

	return tables, nil
}

// 修改获取表创建SQL的方法
// getPrimaryKeyColumns 获取表的所有主键列
func (sbm *SQLBackupManager) getPrimaryKeyColumns(tableName string) ([]string, error) {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_schema = 'public'
		AND tc.table_name = ?
		ORDER BY kcu.ordinal_position
	`

	var columnNames []string
	err := sbm.db.Raw(query, tableName).Pluck("column_name", &columnNames).Error
	if err != nil {
		return nil, err
	}

	return columnNames, nil
}

// 修改获取表创建SQL的方法 - 包含主键定义
func (sbm *SQLBackupManager) getCreateTableSQL(schemaName, tableName string) (string, error) {
	// 获取表的列信息
	query := `
        SELECT 
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.udt_name,
            CASE 
                WHEN c.data_type = 'USER-DEFINED' AND c.udt_name = 'geometry' THEN 'geometry'
                WHEN c.data_type = 'USER-DEFINED' AND c.udt_name = 'geography' THEN 'geography'
                ELSE c.data_type
            END as actual_data_type
        FROM information_schema.columns c
        WHERE c.table_schema = ? AND c.table_name = ?
        ORDER BY c.ordinal_position
    `

	var columns []struct {
		ColumnName             string         `gorm:"column:column_name"`
		DataType               string         `gorm:"column:data_type"`
		CharacterMaximumLength sql.NullInt64  `gorm:"column:character_maximum_length"`
		NumericPrecision       sql.NullInt64  `gorm:"column:numeric_precision"`
		NumericScale           sql.NullInt64  `gorm:"column:numeric_scale"`
		IsNullable             string         `gorm:"column:is_nullable"`
		ColumnDefault          sql.NullString `gorm:"column:column_default"`
		UdtName                string         `gorm:"column:udt_name"`
		ActualDataType         string         `gorm:"column:actual_data_type"`
	}

	if err := sbm.db.Raw(query, schemaName, tableName).Scan(&columns).Error; err != nil {
		return "", err
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("表 %s.%s 不存在或没有列", schemaName, tableName)
	}

	// 获取主键信息
	primaryKeyColumns, err := sbm.getPrimaryKeyColumns(tableName)
	if err != nil {
		log.Printf("获取表%s主键信息失败: %v", tableName, err)
	}

	// 构建CREATE TABLE语句
	var createSQL strings.Builder
	createSQL.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n", tableName))

	columnDefs := make([]string, 0, len(columns))

	for _, col := range columns {
		columnDef := sbm.buildColumnDefinition(tableName, col.ColumnName, col.ActualDataType, col.UdtName,
			col.CharacterMaximumLength, col.NumericPrecision, col.NumericScale,
			col.IsNullable, col.ColumnDefault)
		columnDefs = append(columnDefs, "    "+columnDef)
	}

	// 如果有主键，添加主键约束
	if len(primaryKeyColumns) > 0 {
		primaryKeyDef := fmt.Sprintf("    PRIMARY KEY (%s)", strings.Join(primaryKeyColumns, ", "))
		columnDefs = append(columnDefs, primaryKeyDef)
	}

	createSQL.WriteString(strings.Join(columnDefs, ",\n"))
	createSQL.WriteString("\n);")

	return createSQL.String(), nil
}

// 新增：构建列定义的方法
func (sbm *SQLBackupManager) buildColumnDefinition(tableName, columnName, dataType, udtName string,
	maxLength, precision, scale sql.NullInt64, isNullable string, columnDefault sql.NullString) string {

	var columnDef strings.Builder
	columnDef.WriteString(columnName + " ")

	// 处理数据类型
	switch {
	case dataType == "geometry":
		// 获取几何类型的详细信息
		geomType, srid := sbm.getGeometryTypeInfo(tableName, columnName)
		if geomType != "" && srid > 0 {
			columnDef.WriteString(fmt.Sprintf("geometry(%s,%d)", geomType, srid))
		} else {
			columnDef.WriteString("geometry")
		}
	case dataType == "geography":
		// 获取地理类型的详细信息
		geogType, srid := sbm.getGeographyTypeInfo(columnName)
		if geogType != "" && srid > 0 {
			columnDef.WriteString(fmt.Sprintf("geography(%s,%d)", geogType, srid))
		} else {
			columnDef.WriteString("geography")
		}
	case dataType == "character varying":
		if maxLength.Valid {
			columnDef.WriteString(fmt.Sprintf("character varying(%d)", maxLength.Int64))
		} else {
			columnDef.WriteString("character varying")
		}
	case dataType == "numeric":
		if precision.Valid && scale.Valid {
			columnDef.WriteString(fmt.Sprintf("numeric(%d,%d)", precision.Int64, scale.Int64))
		} else if precision.Valid {
			columnDef.WriteString(fmt.Sprintf("numeric(%d)", precision.Int64))
		} else {
			columnDef.WriteString("numeric")
		}
	default:
		columnDef.WriteString(dataType)
	}

	// 处理NOT NULL约束
	if isNullable == "NO" {
		columnDef.WriteString(" NOT NULL")
	}

	// 处理默认值
	if columnDefault.Valid && columnDefault.String != "" {
		columnDef.WriteString(" DEFAULT " + columnDefault.String)
	}

	return columnDef.String()
}

// 新增：获取几何类型信息
func (sbm *SQLBackupManager) getGeometryTypeInfo(tableName, columnName string) (string, int) {
	// 方案1：完整查询，包含表名
	query := `
        SELECT type, srid 
        FROM geometry_columns 
        WHERE f_table_name = ? AND f_geometry_column = ?
        LIMIT 1
    `

	var geomType string
	var srid int

	if err := sbm.db.Raw(query, tableName, columnName).Row().Scan(&geomType, &srid); err != nil {
		log.Printf("从geometry_columns查询失败，尝试备用方案: %v", err)

		// 方案2：备用查询 - 直接从表中获取几何类型信息
		backupQuery := fmt.Sprintf(`
            SELECT DISTINCT 
                ST_GeometryType(%s) as geom_type,
                ST_SRID(%s) as srid
            FROM %s 
            WHERE %s IS NOT NULL 
            LIMIT 1
        `, columnName, columnName, tableName, columnName)

		var stGeomType string
		if err2 := sbm.db.Raw(backupQuery).Row().Scan(&stGeomType, &srid); err2 != nil {
			log.Printf("备用查询也失败: %v", err2)
			return "", 0
		}

		// 转换ST_GeometryType的结果格式 (ST_MultiPolygon -> MULTIPOLYGON)
		if strings.HasPrefix(stGeomType, "ST_") {
			geomType = strings.ToUpper(strings.TrimPrefix(stGeomType, "ST_"))
		} else {
			geomType = strings.ToUpper(stGeomType)
		}

		log.Printf("通过备用方案获取到几何类型: %s, SRID: %d", geomType, srid)
		return geomType, srid
	}

	log.Printf("获取到几何类型: %s, SRID: %d", geomType, srid)
	return geomType, srid
}

// 新增：获取地理类型信息
func (sbm *SQLBackupManager) getGeographyTypeInfo(columnName string) (string, int) {
	// 从 geography_columns 表获取地理类型信息
	query := `
        SELECT type, srid 
        FROM geography_columns 
        WHERE f_geography_column = ?
        LIMIT 1
    `

	var geogType string
	var srid int

	if err := sbm.db.Raw(query, columnName).Row().Scan(&geogType, &srid); err != nil {
		return "", 4326 // 默认使用WGS84
	}

	return geogType, srid
}

// getTableColumns 获取表的列信息
func (sbm *SQLBackupManager) getTableColumns(schema, tableName string) ([]ColumnInfo, error) {
	var columns []ColumnInfo

	query := `
        SELECT 
            column_name,
            data_type,
            is_nullable,
            COALESCE(column_default, '') as column_default,
            COALESCE(character_maximum_length, 0) as character_maximum_length
        FROM information_schema.columns 
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
    `

	if err := sbm.db.Raw(query, schema, tableName).Scan(&columns).Error; err != nil {
		return nil, err
	}

	return columns, nil
}

// backupTableData 备份表数据
func (sbm *SQLBackupManager) backupTableData(backupDir string) error {
	log.Printf("备份表数据...")

	tables, err := sbm.getAllTables()
	if err != nil {
		return err
	}

	dataDir := filepath.Join(backupDir, "data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	for _, table := range tables {
		// 跳过以"mvt"结尾的表名
		if strings.HasSuffix(strings.ToLower(table.TableName), "mvt") {
			log.Printf("跳过MVT表数据备份: %s", table.TableName)
			continue
		}

		// 跳过spatial_ref_sys表
		if strings.ToLower(table.TableName) == "spatial_ref_sys" {
			log.Printf("跳过spatial_ref_sys表数据备份: %s", table.TableName)
			continue
		}

		if err := sbm.backupSingleTable(dataDir, table.TableName); err != nil {
			log.Printf("备份表%s失败: %v", table.TableName, err)
			continue
		}
		log.Printf("已备份表: %s", table.TableName)
	}

	return nil
}

// backupSingleTable 备份单个表的数据
func (sbm *SQLBackupManager) backupSingleTable(dataDir, tableName string) error {
	// 检查表是否有数据
	var count int64
	if err := sbm.db.Raw(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&count).Error; err != nil {
		return err
	}

	if count == 0 {
		log.Printf("表%s为空，跳过数据备份", tableName)
		return nil
	}

	// 使用INSERT语句方式备份
	return sbm.backupTableAsInserts(dataDir, tableName)
}

// backupTableAsInserts 以INSERT语句形式备份表数据 - 修复版本
func (sbm *SQLBackupManager) backupTableAsInserts(dataDir, tableName string) error {
	dataFile := filepath.Join(dataDir, fmt.Sprintf("%s_data.sql", tableName))
	file, err := os.Create(dataFile)
	if err != nil {
		return err
	}
	defer file.Close()

	// 写入文件头
	file.WriteString(fmt.Sprintf("-- Data for table: %s\n", tableName))
	file.WriteString(fmt.Sprintf("-- Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	// 获取列信息
	columns, err := sbm.getTableColumns("public", tableName)
	if err != nil {
		return err
	}

	columnNames := make([]string, len(columns))
	columnTypes := make(map[string]string)
	for i, col := range columns {
		columnNames[i] = col.ColumnName
		columnTypes[col.ColumnName] = col.DataType
	}

	// 获取主键或唯一标识列用于排序
	orderByColumn, err := sbm.getPrimaryKeyColumn(tableName)
	if err != nil || orderByColumn == "" {
		// 如果没有主键，尝试使用第一个列
		orderByColumn = columnNames[0]
		log.Printf("表%s没有主键，使用列%s进行排序", tableName, orderByColumn)
	}

	// 获取总行数用于进度跟踪
	var totalRows int64
	if err := sbm.db.Raw(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&totalRows).Error; err != nil {
		return err
	}
	log.Printf("开始备份表%s，总行数: %d", tableName, totalRows)

	const batchSize = 1000
	var processedRows int64 = 0
	var lastValue interface{} = nil

	for {
		// 构建查询条件
		var query string
		var args []interface{}

		if lastValue == nil {
			// 第一次查询
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT ?", tableName, orderByColumn)
			args = []interface{}{batchSize}
		} else {
			// 后续查询，使用游标分页
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT ?",
				tableName, orderByColumn, orderByColumn)
			args = []interface{}{lastValue, batchSize}
		}

		// 执行查询
		rows, err := sbm.db.Raw(query, args...).Rows()
		if err != nil {
			return fmt.Errorf("查询表%s数据失败: %v", tableName, err)
		}

		// 获取列名
		dbColumns, err := rows.Columns()
		if err != nil {
			rows.Close()
			return fmt.Errorf("获取列信息失败: %v", err)
		}

		batchCount := 0
		for rows.Next() {
			// 创建接收数据的切片
			values := make([]interface{}, len(dbColumns))
			valuePtrs := make([]interface{}, len(dbColumns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// 扫描数据
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("扫描数据失败: %v", err)
			}

			// 生成INSERT语句
			insertSQL, err := sbm.generateInsertSQL(tableName, dbColumns, values, columnTypes)
			if err != nil {
				rows.Close()
				return fmt.Errorf("生成INSERT语句失败: %v", err)
			}

			file.WriteString(insertSQL + "\n")

			// 更新最后一个值（用于下次查询的游标）
			for i, col := range dbColumns {
				if col == orderByColumn {
					lastValue = values[i]
					break
				}
			}

			batchCount++
			processedRows++
		}

		rows.Close()

		// 如果这批数据少于batchSize，说明已经处理完所有数据
		if batchCount < batchSize {
			break
		}

		// 进度日志
		if processedRows%10000 == 0 {
			log.Printf("表%s备份进度: %d/%d (%.1f%%)",
				tableName, processedRows, totalRows,
				float64(processedRows)/float64(totalRows)*100)
		}
	}

	log.Printf("表%s备份完成，共处理%d行", tableName, processedRows)
	return nil
}

// getPrimaryKeyColumn 获取表的主键列
func (sbm *SQLBackupManager) getPrimaryKeyColumn(tableName string) (string, error) {
	query := `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_schema = 'public'
		AND tc.table_name = ?
		ORDER BY kcu.ordinal_position
		LIMIT 1
	`

	var columnName string
	err := sbm.db.Raw(query, tableName).Scan(&columnName).Error
	if err != nil {
		return "", err
	}

	return columnName, nil
}

// generateInsertSQL 生成INSERT语句
func (sbm *SQLBackupManager) generateInsertSQL(tableName string, columns []string, values []interface{}, columnTypes map[string]string) (string, error) {
	var valueStrings []string

	for i, value := range values {
		columnName := columns[i]
		columnType := columnTypes[columnName]

		if value == nil {
			valueStrings = append(valueStrings, "NULL")
		} else {
			formattedValue, err := sbm.formatValue(value, columnType)
			if err != nil {
				return "", fmt.Errorf("格式化值失败，列%s: %v", columnName, err)
			}
			valueStrings = append(valueStrings, formattedValue)
		}
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(valueStrings, ", "))

	return insertSQL, nil
}

// formatValue 格式化值
func (sbm *SQLBackupManager) formatValue(value interface{}, dataType string) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	switch v := value.(type) {
	case []byte:
		// 处理二进制数据和几何数据
		if dataType == "geometry" || dataType == "geography" {
			// 几何数据使用十六进制格式
			return fmt.Sprintf("'\\x%x'::geometry", v), nil
		}
		// 其他二进制数据
		return fmt.Sprintf("'\\x%x'", v), nil

	case string:
		// 转义单引号
		escaped := strings.ReplaceAll(v, "'", "''")
		return fmt.Sprintf("'%s'", escaped), nil

	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.000000")), nil

	case bool:
		if v {
			return "true", nil
		}
		return "false", nil

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v), nil

	case float32, float64:
		return fmt.Sprintf("%v", v), nil

	default:
		// 其他类型转为字符串并转义
		str := fmt.Sprintf("%v", v)
		escaped := strings.ReplaceAll(str, "'", "''")
		return fmt.Sprintf("'%s'", escaped), nil
	}
}

// backupIndexesAndConstraints 备份索引和约束
func (sbm *SQLBackupManager) backupIndexesAndConstraints(backupDir string) error {
	log.Printf("备份索引和约束...")

	constraintsFile := filepath.Join(backupDir, "constraints.sql")
	file, err := os.Create(constraintsFile)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString("-- Indexes and Constraints\n\n")

	// 备份主键约束
	if err := sbm.backupPrimaryKeys(file); err != nil {
		log.Printf("备份主键失败: %v", err)
	}

	// 备份外键约束
	if err := sbm.backupForeignKeys(file); err != nil {
		log.Printf("备份外键失败: %v", err)
	}

	// 备份索引
	if err := sbm.backupIndexes(file); err != nil {
		log.Printf("备份索引失败: %v", err)
	}

	return nil
}

// backupPrimaryKeys 备份主键
func (sbm *SQLBackupManager) backupPrimaryKeys(file *os.File) error {
	// 由于主键已经在CREATE TABLE时创建，这里可以选择跳过
	// 或者只备份那些需要单独添加的主键约束

	file.WriteString("-- Primary Keys (already created with tables)\n")
	file.WriteString("-- Primary key constraints are included in table creation\n\n")

	return nil
}

// backupForeignKeys 备份外键
func (sbm *SQLBackupManager) backupForeignKeys(file *os.File) error {
	query := `
        SELECT 
            tc.table_name,
            tc.constraint_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = 'public'
    `

	var foreignKeys []struct {
		TableName         string `gorm:"column:table_name"`
		ConstraintName    string `gorm:"column:constraint_name"`
		ColumnName        string `gorm:"column:column_name"`
		ForeignTableName  string `gorm:"column:foreign_table_name"`
		ForeignColumnName string `gorm:"column:foreign_column_name"`
	}

	if err := sbm.db.Raw(query).Scan(&foreignKeys).Error; err != nil {
		return err
	}

	file.WriteString("-- Foreign Keys\n")
	for _, fk := range foreignKeys {
		sql := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s);\n",
			fk.TableName, fk.ConstraintName, fk.ColumnName, fk.ForeignTableName, fk.ForeignColumnName)
		file.WriteString(sql)
	}
	file.WriteString("\n")

	return nil
}

// backupIndexes 备份索引
func (sbm *SQLBackupManager) backupIndexes(file *os.File) error {
	query := `
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE schemaname = 'public'
        AND indexname NOT LIKE '%_pkey'
    `

	var indexes []struct {
		SchemaName string `gorm:"column:schemaname"`
		TableName  string `gorm:"column:tablename"`
		IndexName  string `gorm:"column:indexname"`
		IndexDef   string `gorm:"column:indexdef"`
	}

	if err := sbm.db.Raw(query).Scan(&indexes).Error; err != nil {
		return err
	}

	file.WriteString("-- Indexes\n")
	for _, index := range indexes {
		file.WriteString(index.IndexDef + ";\n")
	}
	file.WriteString("\n")

	return nil
}

// createRestoreScript 创建恢复脚本
func (sbm *SQLBackupManager) createRestoreScript(backupDir string) error {
	restoreScript := filepath.Join(backupDir, "restore.sql")
	file, err := os.Create(restoreScript)
	if err != nil {
		return err
	}
	defer file.Close()

	file.WriteString("-- PostgreSQL Database Restore Script\n")
	file.WriteString(fmt.Sprintf("-- Database: %s\n", sbm.config.Database))
	file.WriteString(fmt.Sprintf("-- Generated: %s\n\n", time.Now().Format("2006-01-02 15:04:05")))

	file.WriteString("-- Step 1: Create tables (with primary keys)\n")
	file.WriteString("\\i schema.sql\n\n")

	file.WriteString("-- Step 2: Insert data\n")

	// 获取所有数据文件
	dataDir := filepath.Join(backupDir, "data")
	files, err := os.ReadDir(dataDir)
	if err == nil {
		// 按表名排序，确保有依赖关系的表按正确顺序恢复
		var dataFiles []string
		for _, f := range files {
			if strings.HasSuffix(f.Name(), "_data.sql") {
				dataFiles = append(dataFiles, f.Name())
			}
		}
		sort.Strings(dataFiles)

		for _, fileName := range dataFiles {
			file.WriteString(fmt.Sprintf("\\i data/%s\n", fileName))
		}
	}

	file.WriteString("\n-- Step 3: Create foreign keys and additional constraints\n")
	file.WriteString("\\i constraints.sql\n\n")

	file.WriteString("-- Step 4: Create indexes\n")
	file.WriteString("-- (Indexes are included in constraints.sql)\n\n")

	file.WriteString("-- Restore completed successfully!\n")

	return nil
}
