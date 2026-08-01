module mysql-manage

go 1.24.0

require github.com/go-sql-driver/mysql v1.10.0

require filippo.io/edwards25519 v1.2.0 // indirect

replace github.com/go-sql-driver/mysql => ./third_party/mysql

replace filippo.io/edwards25519 => ./third_party/edwards25519
