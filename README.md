# 简介

> **注意: 这个库目前并不稳定，请谨慎使用 !**

这是基于 `https://github.com/jackc/pgx` 封装的 Golang `database/sql/driver` 的 `Postgres` 的实现。

目的在于解决以下问题:

1. 大多 Postgres 的库不支持连接池，容易把 Postgres 打爆
2. 对于现有支持连接池的库，无法与 Golang `database/sql/driver` 接口兼容，难以跟主流框架集成.

# 快速上手

1. 在 main.go 中注册该驱动

```
import (
    // pgdriver 将以 postgres 的名字注册进 sql driver
    _ "github.com/Hurricanezwf/pgdriver"
)
```

2. 以 facehook ent 框架为使用案例

```
// # Example DSN
// user=jack password=secret host=pg.example.com port=5432 dbname=mydb sslmode=verify-ca pool_max_conns=10
//
// # Example URL
// postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
func InitPostgres(connString string) (*ent.Client, err) {
    return ent.Open("postgres", connString)
}
```
