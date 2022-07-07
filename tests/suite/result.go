package suite

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

type Row []interface{}

func typeof(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

func (r Row) Equal(columns []string, a Row, num int) bool {
	if len(columns) != len(a) {
		panic("Should be equal")
	}
	if len(r) != len(a) {
		fmt.Printf("len(Values[%d]): %d!=%d \n", num, len(r), len(a))
		return false
	}
	for i := 0; i < len(r); i++ {
		if r[i] == nil && a[i] == nil {
			// do nothing
		} else if (r[i] == nil && a[i] != nil) || (r[i] != nil && a[i] == nil) {
			fmt.Printf("Values[%d] %s: %s!=%s \n", i, columns[i], r[i], a[i])
			return false
		} else {
			//typeX := typeof(r[i])
			//typeY := typeof(a[i])
			//if typeX != typeY{
			//	fmt.Printf("Type error [%d] %s: %s!=%s \n", i, columns[i], typeX, typeY)
			//}
			if x, okx := r[i].(string); okx == true { //string
				if y, oky := a[i].(string); oky == true {
					if x != y {
						fmt.Printf("Values[%d] %s: %s!=%s \n", i, columns[i], x, y)
						return false
					}
				} else {
					panic(columns[i])
				}
			} else if x, okx := r[i].(bool); okx == true { //bool
				if y, oky := a[i].(bool); oky == true {
					if x != y {
						fmt.Printf("Values[%d] %s: %t!=%t \n", i, columns[i], x, y)
						return false
					}
				} else {
					panic(columns[i])
				}
			} else { //float & int
				x := toFloat64(r[i])
				y := toFloat64(a[i])
				if math.Abs(x-y) > 0.000001 {
					fmt.Printf("Values[%d] %s: %g!=%g \n", i, columns[i], x, y)
				}
			}
		}
	}
	return true
}

type Series struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Values  []Row    `json:"values"`
}

type Result struct {
	StatementId int      `json:"statement_id"`
	Series      []Series `json:"series"`
}

type Results struct {
	Results []Result `json:"results"`
}

func (r *Results) Unmarshal(str string) error {
	return json.Unmarshal([]byte(str), r)
}

func (r *Results) Equal(a Results) bool {
	if len(r.Results) != len(a.Results) {
		fmt.Printf("len(Results): %d!=%d \n", len(r.Results), len(a.Results))
		return false
	}
	for i := 0; i < len(r.Results); i++ {
		if r.Results[i].StatementId != a.Results[i].StatementId {
			fmt.Printf("StatementId: %d!=%d \n", r.Results[i].StatementId, a.Results[i].StatementId)
			return false
		}
		if len(r.Results[i].Series) != len(a.Results[i].Series) {
			fmt.Printf("len(Series): %d!=%d \n", len(r.Results[i].Series), len(a.Results[i].Series))
			return false
		}
		for j := 0; j < len(r.Results[i].Series); j++ {
			// Name
			if r.Results[i].Series[j].Name != a.Results[i].Series[j].Name {
				fmt.Printf("Name: %s!=%s \n", r.Results[i].Series[j].Name, a.Results[i].Series[j].Name)
				return false
			}
			// Columns
			if len(r.Results[i].Series[j].Columns) != len(a.Results[i].Series[j].Columns) {
				fmt.Printf("len(Columns): %d!=%d \n",
					len(r.Results[i].Series[j].Columns), len(a.Results[i].Series[j].Columns))
				return false
			}
			for k := 0; k < len(r.Results[i].Series[j].Columns); k++ {
				if r.Results[i].Series[j].Columns[k] != a.Results[i].Series[j].Columns[k] {
					fmt.Printf("Columns[%d]: %s!=%s \n", k,
						r.Results[i].Series[j].Columns[k], a.Results[i].Series[j].Columns[k])
					return false
				}
			}
			// Values
			if len(r.Results[i].Series[j].Values) != len(a.Results[i].Series[j].Values) {
				fmt.Printf("len(Values): %d!=%d \n",
					len(r.Results[i].Series[j].Values), len(a.Results[i].Series[j].Values))
				return false
			}
			for l := 0; l < len(r.Results[i].Series[j].Values); l++ {
				if len(r.Results[i].Series[j].Values[l]) != len(a.Results[i].Series[j].Values[l]) {
					fmt.Printf("len(Values[%d]): %d!=%d \n", l,
						len(r.Results[i].Series[j].Values[l]), len(a.Results[i].Series[j].Values[l]))
					return false
				}
				x := r.Results[i].Series[j].Values[l]
				y := a.Results[i].Series[j].Values[l]
				if !x.Equal(r.Results[i].Series[j].Columns, y, l) {
					return false
				}
			}
		}
	}
	return true
}

func (r *Results) AssertEqual(t *testing.T, a Results) {
	if !r.Equal(a) {
		fmt.Print("A: ")
		fmt.Println(r)
		fmt.Print("B: ")
		fmt.Println(a)
		t.Error("A should be equal to B.")
	}
}

func (r *Results) AssertNotEqual(t *testing.T, a Results) {
	if r.Equal(a) {
		fmt.Print("A: ")
		fmt.Println(r)
		fmt.Print("B: ")
		fmt.Println(a)
		t.Error("A should be not equal to B.")
	}
}

func (r *Results) ToCode(name string) {
	buf := bytes.Buffer{}
	tmp := fmt.Sprintf(`
%s := suite.Results{
	Results: []suite.Result{`, name)
	buf.WriteString(tmp)
	for _, res := range r.Results {
		tmp = `
		{`
		buf.WriteString(tmp)
		tmp = fmt.Sprintf(`
			StatementId: %d,`, res.StatementId)
		buf.WriteString(tmp)
		tmp = `
			Series: []suite.Series{`
		buf.WriteString(tmp)
		for _, s := range res.Series {
			tmp = `
				{`
			buf.WriteString(tmp)
			tmp = fmt.Sprintf(`
					Name: "%s",`, s.Name)
			buf.WriteString(tmp)
			tmp = `
					Columns: []string{`
			buf.WriteString(tmp)
			for i, c := range s.Columns {
				tmp = fmt.Sprintf(`"%s", `, c)
				if i == len(s.Columns)-1 {
					tmp = fmt.Sprintf(`"%s"},`, c)
				}
				buf.WriteString(tmp)
			}
			tmp = `
					Values: []suite.Row{`
			buf.WriteString(tmp)
			for _, row := range s.Values {
				for i, v := range row {
					var tmp1 string
					switch v.(type) {
					case string:
						tmp1 = fmt.Sprintf(`"%s"`, v)
					default:
						if v != nil {
							tmp1 = fmt.Sprintf(`%v`, v)
						} else {
							tmp1 = "nil"
						}
					}
					switch i {
					case 0:
						if len(row) != 1 {
							tmp = fmt.Sprintf(`
						{%s, `, tmp1)
						} else {
							tmp = fmt.Sprintf(`
						{%s}, `, tmp1)
						}
					case len(row) - 1:
						tmp = fmt.Sprintf(`%s},`, tmp1)
					default:
						tmp = fmt.Sprintf(`%s, `, tmp1)
					}
					buf.WriteString(tmp)
				}
			}
			tmp = `
					},`
			buf.WriteString(tmp)
			tmp = `
				},`
			buf.WriteString(tmp)
		}
		tmp = `
			},`
		buf.WriteString(tmp)
		tmp = `
		},`
		buf.WriteString(tmp)
	}
	tmp = `
	},
}`
	buf.WriteString(tmp)
	fmt.Println(buf.String())
}

func toFloat64(a interface{}) float64 {
	switch a.(type) {
	case float64:
		return a.(float64)
	case float32:
		return float64(a.(float32))
	case int:
		return float64(a.(int))
	default:
		panic(a)
	}
}
