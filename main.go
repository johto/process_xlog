package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

const XLOG_DATA_FNAME_LEN int = 24

var dryRun bool

func printUsage() {
	fmt.Fprintf(os.Stderr, `processes files in an xlog archive populated by pg_receivexlog

Usage:
  %s [options] <xlogdir> <process command>

  In the process command, %p is replaced by the path to the file it should
  process, %f is replaced by only the filename, and %% will be replaced with
  a percent sign.  The command should return a zero exit status only if it
  succeeds.
Options:
  --dryrun    dry run, show what the program would do
  --help      display this help
`, os.Args[0])
}

func processFile(dir string, filename string, processCommand string) (err error) {
	processCommand, err = replaceFormatVerbs(processCommand, path.Join(dir, filename), filename)
	if err != nil {
		return err
	}
	if dryRun {
		fmt.Printf("would process %s in %s by running `%s`\n", filename, dir, processCommand)
		return nil
	}
	cmd := exec.Command("sh", "-c", processCommand)
	var captureStderr bytes.Buffer
	cmd.Stderr = &captureStderr
	err = cmd.Run()
	if err != nil {
		log.Printf("process command failed: %s", err.Error())
		if len(captureStderr.Bytes()) > 0 {
			log.Printf("Program output:")
			log.Printf("%s", captureStderr.Bytes())
		}
		os.Exit(1)
	}
	return nil
}

func filterNonXlogFiles(filenames []string) []string {
	var result []string
	for _, file := range(filenames) {
		if len(file) != XLOG_DATA_FNAME_LEN {
			continue
		}

		// must only contain hex characters
		invalidCharacter := func (r rune) bool {
			if (r >= 'A' && r <= 'F') ||
			   (r >= '0' && r <= '9') {
				return false
			}
			return true
		}
		if strings.IndexFunc(file, invalidCharacter) > -1 {
			continue
		}

		result = append(result, file)
	}
	return result
}

func replaceFormatVerbs(format string, fullPath string, filename string) (string, error) {
	var result string
	percent := false
	for _, r := range(format) {
		if !percent {
			if r == '%' {
				percent = true
			} else {
				result = result + string(r)
			}
			continue
		}
		percent = false
		if r == '%' {
			result = result + string(r)
			continue
		}
		switch (r) {
		case 'p':
			result = result + fullPath
		case 'f':
			result = result + filename
		default:
			return "", fmt.Errorf("unrecognized format verb %q", r)
		}
	}
	if percent {
		return "", fmt.Errorf("unterminated format verb")
	}
	return result, nil
}

func main() {
	var displayHelp bool

	log.SetOutput(os.Stderr)
	flagSet := flag.NewFlagSet("args", flag.ExitOnError)
	flagSet.BoolVar(&dryRun, "dryrun", false, "wtf")
	flagSet.BoolVar(&displayHelp, "help", false, "wtf^2")
	flagSet.Usage = printUsage
	err := flagSet.Parse(os.Args[1:])
	if err != nil {
		log.Fatalf("could not parse command-line arguments: %s", err.Error())
	}
	if displayHelp {
		printUsage()
		os.Exit(0)
	}
	if len(flagSet.Args()) != 2 {
		printUsage()
		os.Exit(1)
	}

	dirfh, err := os.Open(flagSet.Arg(0))
	if err != nil {
		log.Fatalf("could not open directory %s: %s", os.Args[1], err.Error())
	}
	filenames, err := dirfh.Readdirnames(0)
	if err != nil {
		log.Fatalf("could not read directory %s: %s", os.Args[1], err.Error())
	}
	dirfh.Close()

	// Compress all files, except the one that's the most recent in the WAL
	// stream.  If we process all files, pg_receivexlog won't know where
	// to start streaming from and we end up with a gap in the WAL stream.
	// pg_receivexlog also does not pay attention to partial files, so it
	// has to be the latest non-partial file.
	filenames = filterNonXlogFiles(filenames)
	if len(filenames) < 2 {
		// nothing to do
		os.Exit(0)
	}
	latestFile := filenames[0]
	for _, file := range(filenames[1:]) {
		var process string
		cmp := bytes.Compare([]byte(latestFile), []byte(file))
		if cmp < 0 {
			process = latestFile
			latestFile = file
		} else if cmp > 0 {
			process = file
		} else {
			log.Fatalf("file %s equal to %s, aborting", latestFile, file)
		}

		processFile(flagSet.Arg(0), process, flagSet.Arg(1))
	}
	if dryRun {
		fmt.Printf("would not process %s\n", latestFile)
	}
}
