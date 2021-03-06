package main

import (
	"nakevaleng/engine/coreconf"
	"nakevaleng/engine/wrappereng"
	"nakevaleng/engine/wrappertest"
)

func main() {
	conf, err := coreconf.LoadConfig("conf.yaml")
	if err != nil {
		panic(err)
	}
	eng := wrappereng.New(conf)
	testCLI(&eng)
}

func testCLI(eng *wrappereng.WrapperEngine) {
	// To remove all the debug output that's written on the CLI, search for:
	// fmt.Println("[DBG]\t

	user := "admin"
	cli := wrappertest.NewCLI(user, eng)

	for cli.IsRunning() {
		cli.Next()
	}
}
