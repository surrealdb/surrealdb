package form

import (
	"errors"
	"fmt"
	"sort"

	diff "github.com/abcum/surreal/util/diff"
)

func NewAsciiFormatter(left map[string]interface{}) *AsciiFormatter {
	return &AsciiFormatter{
		left:           left,
		ShowArrayIndex: false,
	}
}

type AsciiFormatter struct {
	left           map[string]interface{}
	ShowArrayIndex bool
	buffer         string
	path           []string
	size           []int
	inArray        []bool
}

func (f *AsciiFormatter) Format(diff diff.Diff) (result string, err error) {
	f.buffer = ""
	f.path = []string{}
	f.size = []int{}
	f.inArray = []bool{}

	f.printIndent(AsciiSame)
	f.println("{")
	f.push("ROOT", len(f.left), false)
	f.processObject(f.left, diff.Deltas())
	f.pop()
	f.printIndent(AsciiSame)
	f.println("}")

	return f.buffer, nil
}

func (f *AsciiFormatter) processArray(array []interface{}, deltas []diff.Delta) error {
	patchedIndex := 0
	for index, value := range array {
		f.processItem(value, deltas, diff.Index(index))
		patchedIndex++
	}

	// additional Added
	for _, delta := range deltas {
		switch delta.(type) {
		case *diff.Added:
			d := delta.(*diff.Added)
			// skip items already processed
			if int(d.Position.(diff.Index)) < len(array) {
				continue
			}
			f.printRecursive(d.Position.String(), d.Value, AsciiAdded)
		}
	}

	return nil
}

func (f *AsciiFormatter) processObject(object map[string]interface{}, deltas []diff.Delta) error {
	names := sortedKeys(object)
	for _, name := range names {
		value := object[name]
		f.processItem(value, deltas, diff.Name(name))
	}

	// Added
	for _, delta := range deltas {
		switch delta.(type) {
		case *diff.Added:
			d := delta.(*diff.Added)
			f.printRecursive(d.Position.String(), d.Value, AsciiAdded)
		}
	}

	return nil
}

func (f *AsciiFormatter) processItem(value interface{}, deltas []diff.Delta, position diff.Position) error {
	matchedDeltas := f.searchDeltas(deltas, position)
	positionStr := position.String()
	if len(matchedDeltas) > 0 {
		for _, matchedDelta := range matchedDeltas {

			switch matchedDelta.(type) {
			case *diff.Object:
				d := matchedDelta.(*diff.Object)
				switch value.(type) {
				case map[string]interface{}:
					//ok
				default:
					return errors.New("Type mismatch")
				}
				o := value.(map[string]interface{})

				f.printKeyWithIndent(positionStr, AsciiSame)
				f.println("{")
				f.push(positionStr, len(o), false)
				f.processObject(o, d.Deltas)
				f.pop()
				f.printIndent(AsciiSame)
				f.print("}")
				f.printComma()

			case *diff.Array:
				d := matchedDelta.(*diff.Array)
				switch value.(type) {
				case []interface{}:
					//ok
				default:
					return errors.New("Type mismatch")
				}
				a := value.([]interface{})

				f.printKeyWithIndent(positionStr, AsciiSame)
				f.println("[")
				f.push(positionStr, len(a), true)
				f.processArray(a, d.Deltas)
				f.pop()
				f.printIndent(AsciiSame)
				f.print("]")
				f.printComma()

			case *diff.Added:
				d := matchedDelta.(*diff.Added)
				f.printRecursive(positionStr, d.Value, AsciiAdded)
				f.size[len(f.size)-1]++

			case *diff.Modified:
				d := matchedDelta.(*diff.Modified)
				savedSize := f.size[len(f.size)-1]
				f.printRecursive(positionStr, d.OldValue, AsciiDeleted)
				f.size[len(f.size)-1] = savedSize
				f.printRecursive(positionStr, d.NewValue, AsciiAdded)

			case *diff.TextDiff:
				savedSize := f.size[len(f.size)-1]
				d := matchedDelta.(*diff.TextDiff)
				f.printRecursive(positionStr, d.OldValue, AsciiDeleted)
				f.size[len(f.size)-1] = savedSize
				f.printRecursive(positionStr, d.NewValue, AsciiAdded)

			case *diff.Deleted:
				d := matchedDelta.(*diff.Deleted)
				f.printRecursive(positionStr, d.Value, AsciiDeleted)

			default:
				return errors.New("Unknown Delta type detected")
			}

		}
	} else {
		f.printRecursive(positionStr, value, AsciiSame)
	}

	return nil
}

func (f *AsciiFormatter) searchDeltas(deltas []diff.Delta, postion diff.Position) (results []diff.Delta) {
	results = make([]diff.Delta, 0)
	for _, delta := range deltas {
		switch delta.(type) {
		case diff.PostDelta:
			if delta.(diff.PostDelta).PostPosition() == postion {
				results = append(results, delta)
			}
		case diff.PreDelta:
			if delta.(diff.PreDelta).PrePosition() == postion {
				results = append(results, delta)
			}
		default:
			panic("heh")
		}
	}
	return
}

const (
	AsciiSame    = " "
	AsciiAdded   = "+"
	AsciiDeleted = "-"
)

func (f *AsciiFormatter) push(name string, size int, array bool) {
	f.path = append(f.path, name)
	f.size = append(f.size, size)
	f.inArray = append(f.inArray, array)
}

func (f *AsciiFormatter) pop() {
	f.path = f.path[0 : len(f.path)-1]
	f.size = f.size[0 : len(f.size)-1]
	f.inArray = f.inArray[0 : len(f.inArray)-1]
}

func (f *AsciiFormatter) printIndent(marker string) {
	f.print(marker)
	for n := 0; n < len(f.path); n++ {
		f.print("  ")
	}
}

func (f *AsciiFormatter) printKeyWithIndent(name string, marker string) {
	f.printIndent(marker)
	if !f.inArray[len(f.inArray)-1] {
		f.printf(`"%s": `, name)
	} else if f.ShowArrayIndex {
		f.printf(`%s: `, name)
	}
}

func (f *AsciiFormatter) printComma() {
	f.size[len(f.size)-1]--
	if f.size[len(f.size)-1] > 0 {
		f.println(",")
	} else {
		f.println()
	}
}

func (f *AsciiFormatter) printValue(value interface{}) {
	switch value.(type) {
	case string:
		f.buffer += fmt.Sprintf(`"%s"`, value)
	default:
		f.buffer += fmt.Sprintf(`%#v`, value)
	}
}

func (f *AsciiFormatter) print(a ...interface{}) {
	f.buffer += fmt.Sprint(a...)
}

func (f *AsciiFormatter) printf(format string, a ...interface{}) {
	f.buffer += fmt.Sprintf(format, a...)
}

func (f *AsciiFormatter) println(a ...interface{}) {
	f.buffer += fmt.Sprintln(a...)
}

func (f *AsciiFormatter) printRecursive(name string, value interface{}, marker string) {
	switch value.(type) {
	case map[string]interface{}:
		f.printKeyWithIndent(name, marker)
		f.println("{")

		m := value.(map[string]interface{})
		size := len(m)
		f.push(name, size, false)

		keys := sortedKeys(m)
		for _, key := range keys {
			f.printRecursive(key, m[key], marker)
		}
		f.pop()

		f.printIndent(marker)
		f.print("}")
		f.printComma()
	case []interface{}:
		f.printKeyWithIndent(name, marker)
		f.println("[")

		s := value.([]interface{})
		size := len(s)
		f.push("", size, true)
		for _, item := range s {
			f.printRecursive("", item, marker)
		}
		f.pop()

		f.printIndent(marker)
		f.print("]")
		f.printComma()
	default:
		f.printKeyWithIndent(name, marker)
		f.printValue(value)
		f.printComma()
	}
}

func sortedKeys(m map[string]interface{}) (keys []string) {
	keys = make([]string, 0, len(m))
	for key, _ := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return
}
