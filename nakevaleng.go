package main

import (
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/wrappereng"
	"nakevaleng/engine/wrappertest"
)

func main() {
	eng := wrappereng.New(coreconf.LoadConfig("conf.yaml"))
	test_cli(&eng)
}

func test_cli(eng *wrappereng.WrapperEngine) {
	// To remove all the debug output that's written on the CLI, search for:
	// fmt.Println("[DBG]\t

	user := "admin"
	cli := wrappertest.NewCLI(user, eng)

	for cli.IsRunning() {
		cli.Next()
	}
}
