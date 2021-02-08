module github.com/asecurityteam/chiv

go 1.13

require (
	github.com/aws/aws-sdk-go v1.19.6
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golangci/golangci-lint v1.18.0
	github.com/lib/pq v1.0.0
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.1
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/appengine v1.6.7 // indirect
	gopkg.in/yaml.v2 v2.2.2
)

replace sourcegraph.com/sourcegraph/go-diff => github.com/sourcegraph/go-diff v0.5.1
