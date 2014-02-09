package main

import (
	"testing"
)

func TestFormatVerbs(t *testing.T) {
	path := "<path>"
	filename := "<filename>"
	test := func (fmt, expected string, fail string) {
		result, err := replaceFormatVerbs(fmt, path, filename)
		if (err != nil) != (fail != "") {
			t.Errorf("expected %v, got %v", fail, err)
			return
		}
		if fail != "" && err.Error() != fail {
			t.Errorf("expected %s, got %s", fail, err.Error())
		}
		if result != expected {
			t.Errorf("expected %s, got \"%s\"", expected, result)
		}
	}
	test("%%", "%", "")
	test("%p", "<path>", "")
	test("%f", "<filename>", "")
	test("%%%p/%f%%", "%<path>/<filename>%", "")
	test("%x", "", "unrecognized format verb 'x'")
	test("foo %", "", "unterminated format verb")
}
