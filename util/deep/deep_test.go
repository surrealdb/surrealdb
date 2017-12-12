package deep

import (
	"time"

	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

type Mini struct {
	embedded bool
}

type Test struct {
	PublicNumber  int
	PublicString  string
	PublicStruct  interface{}
	PublicPointr  interface{}
	PublicSlice   []interface{}
	PublicMap     map[string]interface{}
	privateNumber int
	privateString string
	privateStruct interface{}
	privatePointr interface{}
	privateSlice  []interface{}
	privateMap    map[string]interface{}
}

func TestMain(t *testing.T) {

	Convey("Can copy nil", t, func() {
		var item interface{}
		item = nil
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

}

func TestSimple(t *testing.T) {

	Convey("Can copy bool", t, func() {
		item := true
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy int64", t, func() {
		item := int64(1)
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy float64", t, func() {
		item := float64(1)
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy string", t, func() {
		item := "string"
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy time.Time", t, func() {
		item := time.Now()
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

}

func TestSlices(t *testing.T) {

	Convey("Can copy slice of bools", t, func() {
		item := []bool{
			true,
			false,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy slice of int64s", t, func() {
		item := []int64{
			1,
			2,
			3,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy slice of float64s", t, func() {
		item := []float64{
			1,
			2,
			3,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy slice of strings", t, func() {
		item := []string{
			"str",
			"str",
			"str",
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy slice of time.Times", t, func() {
		item := []time.Time{
			time.Now(),
			time.Now(),
			time.Now(),
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy slice of interfaces", t, func() {
		item := []interface{}{
			nil,
			int64(1),
			float64(2),
			"str",
			time.Now(),
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

}

func TestObjects(t *testing.T) {

	Convey("Can copy map of bools", t, func() {
		item := map[string]bool{
			"a": true,
			"b": false,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy map of int64s", t, func() {
		item := map[string]int64{
			"a": 1,
			"b": 2,
			"c": 3,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy map of float64s", t, func() {
		item := map[string]float64{
			"a": 1,
			"b": 2,
			"c": 3,
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy map of strings", t, func() {
		item := map[string]string{
			"a": "str",
			"b": "str",
			"c": "str",
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy map of time.Times", t, func() {
		item := map[string]time.Time{
			"a": time.Now(),
			"b": time.Now(),
			"c": time.Now(),
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

	Convey("Can copy map of interfaces", t, func() {
		item := map[interface{}]interface{}{
			1:    nil,
			"b":  int64(1),
			true: float64(2),
			"d":  "str",
			"e":  time.Now(),
		}
		done := Copy(item)
		So(item, ShouldResemble, done)
	})

}

func TestStructs(t *testing.T) {

	full := Test{
		PublicNumber: 1,
		PublicString: "str",
		PublicStruct: Mini{},
		PublicPointr: &Mini{},
		PublicSlice:  []interface{}{nil, 1, "str", true},
		PublicMap: map[string]interface{}{
			"a": 1,
			"b": "str",
			"c": true,
			"d": time.Now(),
		},
		privateNumber: 1,
		privateString: "str",
		privateStruct: Mini{},
		privatePointr: &Mini{},
		privateSlice:  []interface{}{nil, 1, "str", true},
		privateMap: map[string]interface{}{
			"a": 1,
			"b": "str",
			"c": true,
			"d": time.Now(),
		},
	}

	show := Test{
		PublicNumber: full.PublicNumber,
		PublicString: full.PublicString,
		PublicStruct: full.PublicStruct,
		PublicPointr: full.PublicPointr,
		PublicSlice:  full.PublicSlice,
		PublicMap:    full.PublicMap,
	}

	Convey("Can copy struct", t, func() {
		item := full
		done := Copy(item)
		So(done, ShouldResemble, show)
	})

	Convey("Can copy pointer", t, func() {
		item := &full
		done := Copy(item)
		So(done, ShouldResemble, &show)
	})

	Convey("Can copy slice of structs", t, func() {
		item := []interface{}{full, full}
		done := Copy(item)
		So(done, ShouldResemble, []interface{}{show, show})
	})

	Convey("Can copy slice of pointers", t, func() {
		item := []interface{}{&full, &full}
		done := Copy(item)
		So(done, ShouldResemble, []interface{}{&show, &show})
	})

}
