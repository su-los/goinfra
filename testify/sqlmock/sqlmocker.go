// Package sqlmock 基于 sqlmock 库提供快速构建 mocker 的方法
package sqlmock

//nolint:depguard // 单测模块中允许使用 sqlmock 库
import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type SQLDriver string

const (
	DriverNameMySQL     SQLDriver = "mysql"
	DriverNamePostgres  SQLDriver = "postgres"
	DriverNameSQLite    SQLDriver = "sqlite"
	DriverNameSQLServer SQLDriver = "sqlserver"
)

// NewDialector 根据指定的数据库驱动类型和数据库连接池创建 GORM Dialector
func NewDialector(driver SQLDriver, dbConn gorm.ConnPool) (gorm.Dialector, error) {
	var dialector gorm.Dialector
	switch driver {
	case DriverNameMySQL:
		dialector = mysql.New(mysql.Config{
			Conn:                      dbConn,
			DriverName:                driver.String(),
			SkipInitializeWithVersion: true, // 避免自动查询数据库版本，提升测试性能
		})
	case DriverNamePostgres:
		dialector = postgres.New(postgres.Config{
			Conn:       dbConn,
			DriverName: driver.String(),
		})
	case DriverNameSQLite:
		dialector = sqlite.New(sqlite.Config{
			Conn:       dbConn,
			DriverName: driver.String(),
		})
	case DriverNameSQLServer:
		dialector = sqlserver.New(sqlserver.Config{
			Conn:       dbConn,
			DriverName: driver.String(),
		})
	default:
		return nil, errors.Errorf("unsupported driver: %s", driver)
	}
	return dialector, nil
}

func (d SQLDriver) String() string {
	return string(d)
}

type SQLMocker struct {
	sqlmock.Sqlmock
	db *gorm.DB // 将该实例提供给业务代码使用
}

// NewSQLMocker 创建一个用于测试的 SQLMocker 实例
//
// 最佳实践是每个测试用例单独创建一个 SQLMocker 实例，避免测试用例之间相互影响
func NewSQLMocker(driver SQLDriver) (*SQLMocker, error) {
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sqlmock")
	}

	var dialector gorm.Dialector
	if dialector, err = NewDialector(driver, mockDB); err != nil {
		return nil, errors.Wrap(err, "failed to create gorm dialector")
	}

	conn, err := gorm.Open(dialector, &gorm.Config{
		PrepareStmt: false, // 关闭预编译语句缓存，避免测试用例间相互影响
		Logger:      logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open gorm db")
	}

	return &SQLMocker{
		Sqlmock: mock,
		db:      conn,
	}, nil
}

func (m *SQLMocker) DB() *gorm.DB {
	return m.db
}
