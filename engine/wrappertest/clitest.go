package wrappertest

import (
	"bufio"
	"fmt"
	"nakevaleng/ds/cmsketch"
	hyperloglog "nakevaleng/ds/hll"
	"nakevaleng/engine/wrappereng"
	"os"
	"strconv"
	"strings"
)

type cliState int

const (
	_GOOD     = 0
	_BAD_CMD  = 1
	_BAD_ARGC = 2
	_BAD_ARGV = 3
	_BAD_KEY  = 4
	_ERROR    = 5
)

// CLITest is a proxy between the command line interface, utilizing WrapperEngine.
type CLITest struct {
	eng     *wrappereng.WrapperEngine
	args    []string
	state   cliState
	user    string
	running bool
}

// NewCLI returns a pointer to a new CLITest object.
func NewCLI(user string, eng *wrappereng.WrapperEngine) *CLITest {
	fmt.Println("Type 'help' for a list of commands.")
	return &CLITest{
		eng:     eng,
		args:    make([]string, 0),
		state:   _GOOD,
		user:    user,
		running: true,
	}
}

// IsRunning returns whether or not the CLI is running.
func (cli CLITest) IsRunning() bool {
	return cli.running
}

// Next prepares the CLI for its next command(s).
func (cli *CLITest) Next() {
	// Print error and help

	cli.handleError()

	// Read input

	cli.args = getNewArgs()
	if len(cli.args) == 0 {
		return
	}

	// Map input into function

	funcmap := map[string]func() bool{
		"help": cli.help,
		"put":  cli.put,
		"get":  cli.get,
		"del":  cli.del,
		"hllc": cli.hllc,
		"hll":  cli.hll,
		"cmsc": cli.cmsc,
		"cms":  cli.cms,
		"cmsq": cli.cmsq,
		"quit": cli.quit,
	}

	// Call that function

	cmd := cli.args[0]
	f, has := funcmap[cmd]
	if !has {
		cli.state = _BAD_CMD
		return
	}

	f()
}

func getNewArgs() []string {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	line := scanner.Text()
	return strings.Split(line, " ")
}

func (cli *CLITest) handleError() {
	if cli.state == _GOOD {
		return
	} else if cli.state == _BAD_ARGC {
		fmt.Println("Bad argument count!")
	} else if cli.state == _BAD_ARGV {
		fmt.Println("Bad argument value(s)!")
	} else if cli.state == _BAD_CMD {
		fmt.Println("Command not found!")
	} else if cli.state == _BAD_KEY {
		fmt.Println("Invalid key!")
	} else if cli.state == _ERROR {
		fmt.Println("Unexpected error!")
	}

	fmt.Println("Type 'help' for a list of commands.")

	cli.state = _GOOD
}

func (cli CLITest) cmdHasArgc(n int) bool {
	return len(cli.args)-1 == n
}

func (cli *CLITest) put() bool {
	if !cli.cmdHasArgc(2) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	val := []byte(cli.args[2])
	legalKey := cli.eng.Put(cli.user, key, val)

	if !legalKey {
		cli.state = _BAD_KEY
		return false
	}

	return true
}

func (cli *CLITest) quit() bool {
	if !cli.cmdHasArgc(0) {
		cli.state = _BAD_ARGC
		return false
	}

	cli.running = false
	cli.eng.FlushWALBuffer()
	return true
}

func (cli *CLITest) get() bool {
	if !cli.cmdHasArgc(1) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	rec, found := cli.eng.Get(cli.user, key)

	if !found {
		fmt.Println(key, "not found")
		return false
	} else {
		fmt.Println(rec)
	}

	return true
}

func (cli *CLITest) del() bool {
	if !cli.cmdHasArgc(1) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	found := cli.eng.Delete(cli.user, key)

	if !found {
		fmt.Println(key, "not found")
		return false
	}

	return true
}

func (cli *CLITest) hllc() bool {
	if !cli.cmdHasArgc(2) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	k, err := strconv.Atoi(cli.args[2])

	if err != nil {
		cli.state = _BAD_ARGV
		return false
	}

	hll, err := hyperloglog.New(k)
	if err != nil {
		cli.state = _BAD_ARGV
		return false
	}

	cli.eng.PutHLL(cli.user, key, *hll)

	return true
}

func (cli *CLITest) hll() bool {
	// hll key     is for get
	// hll key val is for set

	if cli.cmdHasArgc(1) {
		key := cli.args[1]
		hll := cli.eng.GetHLL(cli.user, key)
		if hll == nil {
			fmt.Println(key, "not found.")
			return false
		}
		fmt.Println(key, ": est =", hll.Estimate())
		return true
	} else if cli.cmdHasArgc(2) {
		key := cli.args[1]
		val := []byte(cli.args[2])
		hll := cli.eng.GetHLL(cli.user, key)
		if hll == nil {
			fmt.Println(key, "not found.")
			return false
		}
		hll.Add(val)
		cli.eng.PutHLL(cli.user, key, *hll)
		return true
	} else {
		cli.state = _BAD_ARGC
		return false
	}
}

func (cli *CLITest) cmsc() bool {
	if !cli.cmdHasArgc(3) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	e, err := strconv.ParseFloat(cli.args[2], 64)
	if err != nil {
		cli.state = _BAD_ARGV
		return false
	}
	d, err := strconv.ParseFloat(cli.args[3], 64)
	if err != nil {
		cli.state = _BAD_ARGV
		return false
	}

	cms, err := cmsketch.New(e, d)
	if err != nil {
		cli.state = _BAD_ARGV
		return false
	}

	cli.eng.PutCMS(cli.user, key, *cms)

	return true
}

func (cli *CLITest) cms() bool {
	if !cli.cmdHasArgc(2) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	val := []byte(cli.args[2])
	cms := cli.eng.GetCMS(cli.user, key)
	if cms == nil {
		fmt.Println(key, "not found.")
		return false
	}
	cms.Insert(val)
	cli.eng.PutCMS(cli.user, key, *cms)
	return true
}

func (cli *CLITest) cmsq() bool {
	if !cli.cmdHasArgc(2) {
		cli.state = _BAD_ARGC
		return false
	}

	key := cli.args[1]
	val := []byte(cli.args[2])
	cms := cli.eng.GetCMS(cli.user, key)
	if cms == nil {
		fmt.Println(key, "not found.")
		return false
	}
	fmt.Println(key, ": est =", cms.Query(val))
	return true
}

func (cli *CLITest) help() bool {
	fmt.Println()
	fmt.Println("help               -  view list of commands")
	fmt.Println("put  [key] [val]   -  insert record")
	fmt.Println("get  [key]         -  find record by key")
	fmt.Println("del  [key]         -  delete record by key")
	fmt.Println("hllc [key] [k]     -  create HLL object [key] with precision [k] (between 4 and 16)")
	fmt.Println("hll  [key] [val]   -  put element [val] into HLL [key]")
	fmt.Println("hll  [key]         -  get estimate for HLL [key]")
	fmt.Println("cmsc [key] [e] [d] -  create CMS object [key] with epsilon [e] and delta [d] (both between 0.0 and 1.0)")
	fmt.Println("cms  [key] [val]   -  put element [val] into CMS [key]")
	fmt.Println("cmsq [key] [val]   -  get estimate for element [val] in CMS [key]")
	fmt.Println("quit               -  exit program")

	return true
}
