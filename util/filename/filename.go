// Package filename implements various utilities to be used with the on-disk
// structures of the system.
package filename

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/facette/natsort"
)

// Possible extensions for filenames created by nakevaleng, without leading period.
const (
	extensionDb  = "db"
	extensionLog = "log"
)

// FileType is an enum for possible file types. Possible values are Type*.
type FileType int

const (
	TypeBad      FileType = iota - 1
	TypeData              // Data segment
	TypeFilter            // Bloom filter
	TypeIndex             // Index table
	TypeSummary           // Index summary table
	TypeMetadata          // Merkle tree
	TypeLog               // Log
)

// fileTypeAsString is used for conversion between FileType and string.
var fileTypeAsString = [...]string{
	"data",
	"filter",
	"index",
	"summary",
	"metadata",
	"log",
}

// String converts the FileType object into a string representation.
func (ftype FileType) String() string {
	return fileTypeAsString[ftype]
}

// IsSSTable returns true if the file represents a table in an SSTable
func (ftype FileType) IsSSTable() bool {
	return ftype >= TypeData && ftype <= TypeMetadata
}

// Table creates a valid table filename (with relative path) used for SSTables.
//	dbname  `Name of the database (or the program that's using nakevaleng).`
//	level   `Level the table is on, used for compaction. Must be >= 1.`
//	run     `Index of the SSTable of this table. Must be >= 0.`
//	filetype`Which data does the table hold (see the FileType enum).`
func Table(relativePath, dbname string, level, run int, filetype FileType) string {
	if relativePath[len(relativePath)-1:] != "/" {
		panic("Table() :: relativePath must end with '/'")
	}

	fname := table(dbname, level, run, filetype)
	return relativePath + fname
}

// Log creates a valid WAL filename (with relative path).
//	dbname  `Name of the database (or the program that's using nakevaleng).`
//	logno   `Index of the log file in the WAL. Must be >= 0.`
func Log(relativePath, dbname string, logno int) string {
	if relativePath[len(relativePath)-1:] != "/" {
		panic("Table() :: relativePath must end with '/'")
	}

	fname := log(dbname, logno)
	return relativePath + fname
}

// Query fetches engine-level information about the file from its filename (not path).
//	returns:
//		dbname  `Name of the database (or the program that's using nakevaleng).`
//
//	If the file is a table:
//		level   `Level the table is on, used for compaction.`
//		run     `Index of the SSTable of this table.`
//	If the file is a log:
//		level   `Index of the log file in the WAL.`
//		run     `Index of the log file in the WAL.`
//
//		filetype`Which data does the table hold (see the FileType enum).`
func Query(fname string) (dbname string, level, run int, filetype FileType) {
	main := strings.Split(fname, ".") // main[0] = name, main[1] = extension

	if len(main) != 2 {
		dbname = ""
		level = -1
		run = -1
		filetype = TypeBad
		return
	}

	main[0] = main[0][strings.LastIndex(main[0], "/")+1:] // trim path from file name

	strings := strings.Split(main[0], "-")

	extension := main[1]

	if extension == extensionDb {
		dbname = strings[0]
		level, _ = strconv.Atoi(strings[1])
		run, _ = strconv.Atoi(strings[2])
		filetype = toFileType(strings[3])
	} else if extension == extensionLog {
		dbname = strings[0]
		level, _ = strconv.Atoi(strings[1])
		run = level
		filetype = TypeLog
	} else {
		dbname = ""
		level = -1
		run = -1
		filetype = TypeBad
	}

	return
}

// GetLastLevel returns the level of the greatest value at the specified path for the database name.
func GetLastLevel(relativePath, dbname string) int {
	files, err := ioutil.ReadDir(relativePath)
	if err != nil {
		panic(err)
	}

	level := -1

	filesStr := []string{}
	for _, f := range files {
		filesStr = append(filesStr, f.Name())
	}
	natsort.Sort(filesStr)

	for i := len(files) - 1; i >= 0; i-- {
		file := filesStr[i]

		dbgot, thisLevel, _, filetype := Query(file)

		if !filetype.IsSSTable() {
			continue
		}

		if dbgot != dbname {
			panic("GetLastLevelAndRun() :: Bad database names (not matching)!")
		}

		if thisLevel > level {
			level = thisLevel
		}

	}

	return level
}

// GetLastRun returns the run at given level at the specified path for the given database name.
// Returns a number from 0 onwards.
// Returns -1 if the level does not exist (i.e. it's empty)
func GetLastRun(relativepath, dbname string, level int) int {
	if level <= 0 {
		panic("Level must be >= 1")
	}

	files, err := ioutil.ReadDir(relativepath)
	if err != nil {
		panic(err)
	}

	run := -1

	filesStr := []string{}
	for _, f := range files {
		filesStr = append(filesStr, f.Name())
	}
	natsort.Sort(filesStr)

	for i := len(files) - 1; i >= 0; i-- {
		file := filesStr[i]
		dbgot, thislevel, thisRun, filetype := Query(file)

		if !filetype.IsSSTable() {
			continue
		}

		if thislevel != level {
			continue
		}

		if dbgot != dbname {
			panic("GetRun() :: Bad database names (not matching)!")
		}

		if thisRun > run {
			run = thisRun
		}
	}

	return run
}

// GetLastLog returns the index of the last log file at the specified path for the given database.
func GetLastLog(relativePath string, dbname string) int {
	files, err := ioutil.ReadDir(relativePath)
	if err != nil {
		panic(err)
	}

	logno := -1

	filesStr := []string{}
	for _, f := range files {
		filesStr = append(filesStr, f.Name())
	}
	natsort.Sort(filesStr)

	for i := len(files) - 1; i >= 0; i-- {
		file := filesStr[i]

		dbgot := ""
		filetype := TypeLog
		dbgot, myLogNo, _, filetype := Query(file)

		if filetype != TypeLog {
			continue
		}

		if dbgot != dbname {
			panic("GetLastLog() :: Bad database names (not matching)!")
		}

		if myLogNo > logno {
			logno = myLogNo
		}
	}

	return logno
}

// GetSegmentPaths returns a slice of all the log paths at the specified relative path for the given database.
func GetSegmentPaths(relativePath string, dbname string) []string {
	files, err := ioutil.ReadDir(relativePath)
	if err != nil {
		panic(err)
	}

	filesStr := []string{}
	for _, f := range files {
		filesStr = append(filesStr, f.Name())
	}
	natsort.Sort(filesStr)

	segmentPaths := make([]string, 0)
	for i := 0; i < len(files); i++ {
		file := filesStr[i]

		dbgot := ""
		filetype := TypeLog
		dbgot, myLogNo, _, filetype := Query(file)

		if filetype != TypeLog {
			continue
		}

		if dbgot != dbname {
			panic("GetLastLog() :: Bad database names (not matching)!")
		}

		path := Log(relativePath, dbgot, myLogNo)
		segmentPaths = append(segmentPaths, path)
	}

	return segmentPaths
}

// ToFileType tries to convert a string into a FileType object. Will panic if no string is a match.
func toFileType(s string) FileType {
	for i := 0; i < len(fileTypeAsString); i++ {
		if fileTypeAsString[i] == s {
			return FileType(i)
		}
	}

	panic("Bad database filetype name!")
}

// table creates a valid table filename from provided parameters, used for SSTables.
//	dbname  `Name of the database (or the program that's using nakevaleng).`
//	level   `Level the table is on, used for compaction. Must be >= 1.`
//	run     `Index of the SSTable of this table. Must be >= 0.`
//	filetype`Which data does the table hold (see the FileType enum).`
func table(dbname string, level, run int, filetype FileType) string {
	if level <= 0 {
		panic("Level must be a positive integer!")
	}
	if run < 0 {
		panic("Run must be a non-negative integer!")
	}

	return fmt.Sprintf("%s-%d-%d-%s.%s", dbname, level, run, filetype, extensionDb)
}

// log creates a valid WAL filename from the provided parameters.
//	dbname  `Name of the database (or the program that's using nakevaleng).`
//	logno   `Index of the log file on disk. Must be >= 0.`
func log(dbname string, logno int) string {
	if logno < 0 {
		panic("Level must be a positive integer!")
	}

	return fmt.Sprintf("%s-%d.%s", dbname, logno, extensionLog)
}
